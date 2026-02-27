-- ================================================================
-- DIM_CUSTOMER TABLE : STATIC -SCD TYPE-2 
-- ================================================================
-- Purpose: Use actual Bronze columns + create UDF for loyalty calculation
--   1. AGE (from Bronze AGE column - VARCHAR converted to NUMBER)
--   2. CUSTOMER_GENDER (from Bronze CUSTOMER_GENDER column)
--   3. AGE_GROUP (binned from AGE: 18-25, 26-35, 36-45, 46-55, 56+)
--   4. LOYALTY_TIER (calculated via UDF function)
--   5. UDF: CALCULATE_LOYALTY_TIER(lifetime_value, booking_count)
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- 1: CREATE UDF FUNCTIONS
-- ================================================================

-- Function 1: Calculate Loyalty Tier based on spend AND booking frequency
CREATE OR REPLACE FUNCTION CALCULATE_LOYALTY_TIER(
    lifetime_value NUMBER(10,2),
    total_bookings NUMBER(10,0)
)
RETURNS VARCHAR(20)
COMMENT = 'Calculate customer loyalty tier based on lifetime value AND booking count'
AS
$$
    CASE
        -- Platinum: High spenders (₹50K+) OR very frequent bookers (20+ bookings)
        WHEN lifetime_value >= 50000 OR total_bookings >= 20 THEN 'Platinum'
        
        -- Gold: Mid-high spenders (₹25K+) OR frequent bookers (10+ bookings)
        WHEN lifetime_value >= 25000 OR total_bookings >= 10 THEN 'Gold'
        
        -- Silver: Regular spenders (₹10K+) OR regular bookers (5+ bookings)
        WHEN lifetime_value >= 10000 OR total_bookings >= 5 THEN 'Silver'
        
        -- Bronze: Everyone else
        ELSE 'Bronze'
    END
$$;

SELECT '✅ CALCULATE_LOYALTY_TIER UDF created' AS status;


-- Function 2: Bin AGE into age groups
CREATE OR REPLACE FUNCTION BIN_AGE_GROUP(age_str VARCHAR)
RETURNS VARCHAR(20)
COMMENT = 'Convert age string to age group bins'
AS
$$
    CASE
        WHEN TRY_TO_NUMBER(age_str) IS NULL THEN 'Unknown'
        WHEN TRY_TO_NUMBER(age_str) < 18 THEN 'Under 18'
        WHEN TRY_TO_NUMBER(age_str) BETWEEN 18 AND 25 THEN '18-25'
        WHEN TRY_TO_NUMBER(age_str) BETWEEN 26 AND 35 THEN '26-35'
        WHEN TRY_TO_NUMBER(age_str) BETWEEN 36 AND 45 THEN '36-45'
        WHEN TRY_TO_NUMBER(age_str) BETWEEN 46 AND 55 THEN '46-55'
        WHEN TRY_TO_NUMBER(age_str) >= 56 THEN '56+'
        ELSE 'Unknown'
    END
$$;

SELECT '✅ BIN_AGE_GROUP UDF created' AS status;


-- ================================================================
-- 2: CREATE DIM_CUSTOMER TABLE
-- ================================================================

CREATE OR REPLACE TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER (

    -- SURROGATE KEY (SCD2-safe with HASH)
    CUSTOMER_KEY            NUMBER(38,0)    NOT NULL,
    
    -- NATURAL KEY
    CUSTOMER_ID             VARCHAR(50)     NOT NULL,
    
    -- CUSTOMER ATTRIBUTES
    CUSTOMER_NAME           VARCHAR(200),
    CUSTOMER_EMAIL          VARCHAR(200),
    CUSTOMER_PHONE          VARCHAR(50),
    
    CUSTOMER_GENDER         VARCHAR(20)     COMMENT 'Male, Female, Other (from Bronze)',
    AGE                     NUMBER(3,0)     COMMENT 'Customer age (from Bronze AGE column)',
    AGE_GROUP               VARCHAR(20)     COMMENT '18-25, 26-35, 36-45, 46-55, 56+',
    
    -- Demographics
    CUSTOMER_CITY           VARCHAR(100),
    USER_STATE              VARCHAR(100),
    USER_COUNTRY            VARCHAR(100),
    
    -- Lifecycle
    USER_SIGNUP_DATE        DATE,
    USER_SEGMENT            VARCHAR(50)     COMMENT 'VIP, Regular, New, etc.',
    
    LOYALTY_TIER            VARCHAR(20)     COMMENT 'Bronze, Silver, Gold, Platinum',
    
    USER_LIFETIME_VALUE     NUMBER(10,2)    COMMENT 'Total customer spend',
    PREFERRED_CATEGORY      VARCHAR(100)    COMMENT 'Movies, Dining, Events, etc.',
    
    -- Pass Subscription Status
    HAS_PASS_SUBSCRIPTION   BOOLEAN         DEFAULT FALSE
                            COMMENT 'TRUE if customer ever had Pass subscription',
    
    -- SCD TYPE 2 METADATA
    VERSION_NUMBER          NUMBER(5,0)     NOT NULL DEFAULT 1
                            COMMENT 'Increments each time tracked attribute changes',
    
    EFFECTIVE_DATE          DATE            NOT NULL
                            COMMENT 'Date this version became active',
    
    EXPIRY_DATE             DATE            DEFAULT '9999-12-31'::DATE
                            COMMENT 'Date this version expired (9999-12-31 = current)',
    
    IS_CURRENT              BOOLEAN         NOT NULL DEFAULT TRUE
                            COMMENT 'TRUE only for latest/active version',
    
    CHANGE_REASON           VARCHAR(200)
                            COMMENT 'What changed: CITY_CHANGED, SEGMENT_UPGRADED, etc.',
    
    -- AUDIT
    CREATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE             VARCHAR(500),
    
    -- CONSTRAINTS
    CONSTRAINT pk_dim_customer PRIMARY KEY (CUSTOMER_KEY),
    CONSTRAINT uq_customer_version UNIQUE (CUSTOMER_ID, VERSION_NUMBER)
)
COMMENT = 'Customer dimension - SCD Type 2.';

-- ================================================================
-- 3: CREATE STREAM ON BRONZE
-- ================================================================

CREATE OR REPLACE STREAM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE_STREAM
ON TABLE DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE
SHOW_INITIAL_ROWS = TRUE
COMMENT = 'Captures INSERT/UPDATE changes in Bronze for DIM_CUSTOMER SCD2 processing';

SELECT ' Stream created on Bronze table' AS status;

-- ================================================================
-- 4: INITIAL LOAD PROCEDURE
-- ================================================================

CREATE OR REPLACE PROCEDURE DISTRICT_ANALYTICS_DB.SILVER_LAYER.INIT_DIM_CUSTOMER()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INTEGER DEFAULT 0;
BEGIN
    -- Load initial customer data from Bronze
    INSERT INTO DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER (
        CUSTOMER_KEY,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_EMAIL,
        CUSTOMER_PHONE,
        CUSTOMER_GENDER,
        AGE,
        AGE_GROUP,
        CUSTOMER_CITY,
        USER_STATE,
        USER_COUNTRY,
        USER_SIGNUP_DATE,
        USER_SEGMENT,
        LOYALTY_TIER,
        USER_LIFETIME_VALUE,
        PREFERRED_CATEGORY,
        HAS_PASS_SUBSCRIPTION,
        VERSION_NUMBER,
        EFFECTIVE_DATE,
        EXPIRY_DATE,
        IS_CURRENT,
        CHANGE_REASON,
        SOURCE_FILE
    )
    WITH customer_aggregates AS (
        -- Aggregate booking counts per customer
        SELECT
            TRIM(USER_ID) AS user_id,
            COUNT(DISTINCT BOOKING_ID) AS total_bookings
        FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE
        WHERE USER_ID IS NOT NULL
          AND TRIM(USER_ID) != ''
        GROUP BY TRIM(USER_ID)
    )
    SELECT
        -- HASH-based surrogate key (deterministic, reproducible)
        ABS(HASH(TRIM(raw.USER_ID)))                        AS CUSTOMER_KEY,
        TRIM(raw.USER_ID)                                   AS CUSTOMER_ID,
        TRIM(raw.CUSTOMER_NAME)                             AS CUSTOMER_NAME,
        TRIM(raw.USER_EMAIL)                                AS CUSTOMER_EMAIL,
        TRIM(raw.USER_PHONE)                                AS CUSTOMER_PHONE,
        
        --  CUSTOMER_GENDER: Direct from Bronze
        TRIM(raw.CUSTOMER_GENDER)                           AS CUSTOMER_GENDER,
        
        --  AGE: Convert from Bronze VARCHAR to NUMBER
        TRY_TO_NUMBER(TRIM(raw.AGE))                        AS AGE,
        
        --  AGE_GROUP: Use UDF to bin age
        BIN_AGE_GROUP(TRIM(raw.AGE))                        AS AGE_GROUP,
        
        TRIM(raw.CUSTOMER_CITY)                             AS CUSTOMER_CITY,
        TRIM(raw.USER_STATE)                                AS USER_STATE,
        TRIM(raw.USER_COUNTRY)                              AS USER_COUNTRY,
        TRY_TO_DATE(raw.USER_SIGNUP_DATE)                   AS USER_SIGNUP_DATE,
        TRIM(raw.USER_SEGMENT)                              AS USER_SEGMENT,
        
        --  LOYALTY_TIER: Use UDF with lifetime value AND booking count
        CALCULATE_LOYALTY_TIER(
            raw.USER_LIFETIME_VALUE, 
            COALESCE(agg.total_bookings, 0)
        )                                                   AS LOYALTY_TIER,
        
        raw.USER_LIFETIME_VALUE,
        TRIM(raw.PREFERRED_CATEGORY)                        AS PREFERRED_CATEGORY,
        MAX(CASE WHEN raw.PASS_SUBSCRIPTION_ID IS NOT NULL 
            THEN TRUE ELSE FALSE END) 
            OVER (PARTITION BY TRIM(raw.USER_ID))           AS HAS_PASS_SUBSCRIPTION,
        1                                                   AS VERSION_NUMBER,
        MIN(TRY_TO_DATE(raw.BOOKING_DATE)) 
            OVER (PARTITION BY TRIM(raw.USER_ID))           AS EFFECTIVE_DATE,
        '9999-12-31'::DATE                                  AS EXPIRY_DATE,
        TRUE                                                AS IS_CURRENT,
        'INITIAL_LOAD'                                      AS CHANGE_REASON,
        raw.SOURCE_FILE
    FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE raw
    LEFT JOIN customer_aggregates agg
        ON TRIM(raw.USER_ID) = agg.user_id
    WHERE raw.USER_ID IS NOT NULL
      AND TRIM(raw.USER_ID) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(raw.USER_ID)
        ORDER BY TRY_TO_DATE(raw.BOOKING_DATE) DESC,
                 raw.LOADED_AT DESC
    ) = 1;
    
    rows_inserted := SQLROWCOUNT;
    RETURN '✅ Initial load complete. Rows inserted: ' || rows_inserted;
END;
$$;

SELECT '✅ INIT_DIM_CUSTOMER procedure created' AS status;

-- EXECUTE INITIAL LOAD
CALL DISTRICT_ANALYTICS_DB.SILVER_LAYER.INIT_DIM_CUSTOMER();

-- ================================================================
-- 5: INCREMENTAL LOAD PROCEDURE (SCD2 MERGE)
-- ================================================================
CREATE OR REPLACE PROCEDURE DISTRICT_ANALYTICS_DB.SILVER_LAYER.LOAD_DIM_CUSTOMER()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    has_data BOOLEAN;
    rows_processed INTEGER DEFAULT 0;
BEGIN

    -- Check if stream has data
    has_data := SYSTEM$STREAM_HAS_DATA(
        'DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE_STREAM'
    );

    IF (NOT has_data) THEN
        RETURN 'No new data in stream. Skipping.';
    END IF;

    -- Stage new customer records
    CREATE OR REPLACE TEMPORARY TABLE _CUSTOMER_STAGE AS
    WITH customer_agg AS (
        SELECT
            TRIM(USER_ID) AS user_id,
            COUNT(DISTINCT BOOKING_ID) AS total_bookings
        FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE_STREAM
        WHERE USER_ID IS NOT NULL
          AND TRIM(USER_ID) != ''
          AND METADATA$ACTION = 'INSERT'
        GROUP BY TRIM(USER_ID)
    )
    SELECT
        TRIM(raw.USER_ID)                         AS customer_id,
        TRIM(raw.CUSTOMER_NAME)                   AS customer_name,
        TRIM(raw.USER_EMAIL)                      AS customer_email,
        TRIM(raw.USER_PHONE)                      AS customer_phone,
        
        TRIM(raw.CUSTOMER_GENDER)                 AS customer_gender,
        
        TRY_TO_NUMBER(TRIM(raw.AGE))              AS age,
        
        BIN_AGE_GROUP(TRIM(raw.AGE))              AS age_group,
        
        TRIM(raw.CUSTOMER_CITY)                   AS customer_city,
        TRIM(raw.USER_STATE)                      AS user_state,
        TRIM(raw.USER_COUNTRY)                    AS user_country,
        TRY_TO_DATE(raw.USER_SIGNUP_DATE)         AS user_signup_date,
        TRIM(raw.USER_SEGMENT)                    AS user_segment,
        
        CALCULATE_LOYALTY_TIER(
            raw.USER_LIFETIME_VALUE,
            COALESCE(agg.total_bookings, 0)
        )                                         AS loyalty_tier,
        
        raw.USER_LIFETIME_VALUE,
        TRIM(raw.PREFERRED_CATEGORY)              AS preferred_category,
        MAX(CASE 
            WHEN raw.PASS_SUBSCRIPTION_ID IS NOT NULL 
            THEN TRUE ELSE FALSE 
        END)                                      AS has_pass_subscription,
        TRY_TO_DATE(raw.BOOKING_DATE)             AS booking_date,
        raw.SOURCE_FILE
    FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE_STREAM raw
    LEFT JOIN customer_agg agg
        ON TRIM(raw.USER_ID) = agg.user_id
    WHERE raw.USER_ID IS NOT NULL
      AND TRIM(raw.USER_ID) != ''
      AND raw.METADATA$ACTION = 'INSERT'
    GROUP BY
        TRIM(raw.USER_ID),
        TRIM(raw.CUSTOMER_NAME),
        TRIM(raw.USER_EMAIL),
        TRIM(raw.USER_PHONE),
        TRIM(raw.CUSTOMER_GENDER),
        TRIM(raw.AGE),
        TRIM(raw.CUSTOMER_CITY),
        TRIM(raw.USER_STATE),
        TRIM(raw.USER_COUNTRY),
        TRY_TO_DATE(raw.USER_SIGNUP_DATE),
        TRIM(raw.USER_SEGMENT),
        raw.USER_LIFETIME_VALUE,
        TRIM(raw.PREFERRED_CATEGORY),
        TRY_TO_DATE(raw.BOOKING_DATE),
        raw.SOURCE_FILE,
        agg.total_bookings
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRIM(raw.USER_ID)
        ORDER BY TRY_TO_DATE(raw.BOOKING_DATE) DESC
    ) = 1;

    -- Insert new customers (not in DIM_CUSTOMER yet)
    INSERT INTO DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER (
        CUSTOMER_KEY,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CUSTOMER_EMAIL,
        CUSTOMER_PHONE,
        CUSTOMER_GENDER,
        AGE,
        AGE_GROUP,
        CUSTOMER_CITY,
        USER_STATE,
        USER_COUNTRY,
        USER_SIGNUP_DATE,
        USER_SEGMENT,
        LOYALTY_TIER,
        USER_LIFETIME_VALUE,
        PREFERRED_CATEGORY,
        HAS_PASS_SUBSCRIPTION,
        VERSION_NUMBER,
        EFFECTIVE_DATE,
        EXPIRY_DATE,
        IS_CURRENT,
        CHANGE_REASON,
        SOURCE_FILE
    )
    SELECT
        ABS(HASH(s.customer_id))              AS CUSTOMER_KEY,
        s.customer_id,
        s.customer_name,
        s.customer_email,
        s.customer_phone,
        s.customer_gender,
        s.age,
        s.age_group,
        s.customer_city,
        s.user_state,
        s.user_country,
        s.user_signup_date,
        s.user_segment,
        s.loyalty_tier,
        s.user_lifetime_value,
        s.preferred_category,
        s.has_pass_subscription,
        1                                     AS VERSION_NUMBER,
        s.booking_date                        AS EFFECTIVE_DATE,
        '9999-12-31'::DATE                    AS EXPIRY_DATE,
        TRUE                                  AS IS_CURRENT,
        'NEW_CUSTOMER'                        AS CHANGE_REASON,
        s.SOURCE_FILE
    FROM _CUSTOMER_STAGE s
    WHERE NOT EXISTS (
        SELECT 1 
        FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER d
        WHERE d.CUSTOMER_ID = s.customer_id
    );

    rows_processed := SQLROWCOUNT;
    
    RETURN '✅ LOAD_DIM_CUSTOMER executed. New customers inserted: ' || rows_processed;

END;
$$;

SELECT '✅ LOAD_DIM_CUSTOMER procedure created' AS status;

-- ================================================================
-- STEP 7: CREATE TASK FOR INCREMENTAL LOADS
-- ================================================================

CREATE OR REPLACE TASK DISTRICT_ANALYTICS_DB.SILVER_LAYER.LOAD_DIM_CUSTOMER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE_STREAM')
AS
    CALL DISTRICT_ANALYTICS_DB.SILVER_LAYER.LOAD_DIM_CUSTOMER();

ALTER TASK DISTRICT_ANALYTICS_DB.SILVER_LAYER.LOAD_DIM_CUSTOMER_TASK RESUME;


select * from dim_customer ;
