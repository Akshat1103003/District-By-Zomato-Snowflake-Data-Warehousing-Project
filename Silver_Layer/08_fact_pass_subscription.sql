-- ================================================================
-- FACT_PASS_SUBSCRIPTION - ENHANCED WITH FULL ANALYTICS
-- ================================================================
-- Purpose: Comprehensive Pass subscription analytics table
-- Dependencies: FACT_BOOKINGS, DIM_PASS_PLAN, DIM_DATE
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;


CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_PASS_SUBSCRIPTION
TARGET_LAG   = '5 minutes'
REFRESH_MODE = AUTO
INITIALIZE   = ON_CREATE
WAREHOUSE    = COMPUTE_WH
COMMENT      = 'Enhanced Pass subscription table with comprehensive analytics: revenue, utilization, engagement, segmentation.'
AS
WITH first_benefit_lookup AS (
    -- Get the first benefit type used per subscription
    SELECT
        fb.PASS_SUBSCRIPTION_ID,
        CASE 
            WHEN fb.PASS_TICKET_DISCOUNT > 0 THEN 'Movie'
            WHEN fb.PASS_DINING_VOUCHER > 0 THEN 'Dining'
            WHEN fb.PASS_SNACK_DISCOUNT > 0 THEN 'Snack'
        END AS first_benefit_type
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BOOKINGS fb
    WHERE fb.HAS_PASS_BENEFIT = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fb.PASS_SUBSCRIPTION_ID 
        ORDER BY fb.BOOKING_DATE
    ) = 1
),
subscription_base AS (
    -- Aggregate all metrics per subscription from FACT_BOOKINGS
    SELECT
        fb.PASS_SUBSCRIPTION_ID                             AS subscription_id,
        fb.CUSTOMER_KEY,
        
        -- DATE DERIVATION
        MIN(fb.BOOKING_DATE)                                AS start_date,
        MIN(fb.BOOKING_DATE)                                AS purchase_date,
        
        -- BENEFIT USAGE COUNTS (how many times used)
        SUM(CASE WHEN fb.PASS_TICKET_DISCOUNT > 0 THEN 1 ELSE 0 END) 
                                                            AS movie_benefits_used,
        SUM(CASE WHEN fb.PASS_DINING_VOUCHER > 0 THEN 1 ELSE 0 END) 
                                                            AS dining_benefits_used,
        SUM(CASE WHEN fb.PASS_SNACK_DISCOUNT > 0 THEN 1 ELSE 0 END) 
                                                            AS snack_benefits_used,
        
        -- DISCOUNT AMOUNTS GIVEN (₹ total)
        SUM(COALESCE(fb.PASS_TICKET_DISCOUNT, 0))          AS movie_discounts_given,
        SUM(COALESCE(fb.PASS_DINING_VOUCHER, 0))           AS dining_discounts_given,
        SUM(COALESCE(fb.PASS_SNACK_DISCOUNT, 0))           AS snack_discounts_given,
        SUM(COALESCE(fb.RESTAURANT_INSTANT_DISCOUNT, 0))   AS restaurant_instant_discounts,
        
        -- BOOKING COUNTS (engagement)
        COUNT(DISTINCT fb.BOOKING_ID)                       AS total_bookings,
        SUM(CASE WHEN fb.BOOKING_CATEGORY IN ('Movies', 'Movies - Snacks') 
            THEN 1 ELSE 0 END)                              AS movie_bookings,
        SUM(CASE WHEN fb.BOOKING_CATEGORY = 'Dining' 
            THEN 1 ELSE 0 END)                              AS dining_bookings,
        SUM(CASE WHEN fb.BOOKING_CATEGORY IN ('Movies - Snacks') 
            AND fb.SNACK_AMOUNT > 0 
            THEN 1 ELSE 0 END)                              AS snack_bookings,
        
        -- SPENDING METRICS
        SUM(COALESCE(fb.TOTAL_AMOUNT, 0))                   AS total_spend_during_pass,
        AVG(COALESCE(fb.TOTAL_AMOUNT, 0))                   AS avg_booking_value,
        
        -- REDEMPTION TIMING
        MIN(CASE WHEN fb.HAS_PASS_BENEFIT THEN fb.BOOKING_DATE END) 
                                                            AS first_redemption_date,
        MAX(CASE WHEN fb.HAS_PASS_BENEFIT THEN fb.BOOKING_DATE END) 
                                                            AS last_redemption_date,
        
        -- METADATA
        MAX(fb.PAYMENT_METHOD)                              AS payment_method,
        MAX(fb.SOURCE_FILE)                                 AS source_file
        
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BOOKINGS fb
    WHERE fb.PASS_SUBSCRIPTION_ID IS NOT NULL
    GROUP BY fb.PASS_SUBSCRIPTION_ID, fb.CUSTOMER_KEY
)
SELECT
    -- KEYS
    ABS(HASH(sb.subscription_id))                           AS SUBSCRIPTION_KEY,
    sb.subscription_id                                      AS SUBSCRIPTION_ID,
    sb.CUSTOMER_KEY,
    pp.PASS_PLAN_KEY,
    
    -- PLAN ATTRIBUTES (from DIM_PASS_PLAN)
    pp.PLAN_NAME                                            AS PASS_PLAN_NAME,
    pp.PLAN_PRICE                                           AS PASS_PLAN_PRICE,
    pp.VALIDITY_DAYS                                        AS PASS_PLAN_VALIDITY_DAYS,
    
    -- DATES
    sb.purchase_date                                        AS PURCHASE_DATE,
    sb.start_date                                           AS START_DATE,
    DATEADD('day', pp.VALIDITY_DAYS, sb.start_date)         AS END_DATE,
    
    -- Date dimensions (for joins)
    d_pur.DATE_KEY                                          AS PURCHASE_DATE_KEY,
    d_sta.DATE_KEY                                          AS START_DATE_KEY,
    d_end.DATE_KEY                                          AS END_DATE_KEY,
    
    -- Cohort analysis
    DATE_TRUNC('month', sb.purchase_date)                   AS COHORT_MONTH,
    DATE_TRUNC('quarter', sb.purchase_date)                 AS COHORT_QUARTER,
    YEAR(sb.purchase_date)                                  AS PURCHASE_YEAR,
    MONTHNAME(sb.purchase_date)                             AS PURCHASE_MONTH_NAME,
    
    -- STATUS FLAGS (recalculated on each refresh)
    CASE
        WHEN CURRENT_DATE() < sb.start_date THEN 'Pending'
        WHEN CURRENT_DATE() BETWEEN sb.start_date 
             AND DATEADD('day', pp.VALIDITY_DAYS, sb.start_date) 
        THEN 'Active'
        ELSE 'Expired'
    END                                                     AS SUBSCRIPTION_STATUS,
    
    CASE
        WHEN CURRENT_DATE() BETWEEN sb.start_date 
             AND DATEADD('day', pp.VALIDITY_DAYS, sb.start_date)
        THEN TRUE ELSE FALSE
    END                                                     AS IS_ACTIVE,
    
    CASE
        WHEN CURRENT_DATE() > DATEADD('day', pp.VALIDITY_DAYS, sb.start_date)
        THEN TRUE ELSE FALSE
    END                                                     AS IS_EXPIRED,
    
    DATEDIFF('day', sb.purchase_date, CURRENT_DATE())       AS DAYS_SINCE_PURCHASE,
    GREATEST(
        DATEDIFF('day', CURRENT_DATE(), 
                 DATEADD('day', pp.VALIDITY_DAYS, sb.start_date)), 
        0
    )                                                       AS DAYS_UNTIL_EXPIRY,
    
    -- ══════════════════════════════════════════════════════════
    -- FINANCIAL METRICS
    -- ══════════════════════════════════════════════════════════
    pp.PLAN_PRICE                                           AS PASS_REVENUE,
    
    sb.movie_discounts_given                                AS MOVIE_DISCOUNTS_GIVEN,
    sb.dining_discounts_given                               AS DINING_DISCOUNTS_GIVEN,
    sb.snack_discounts_given                                AS SNACK_DISCOUNTS_GIVEN,
    sb.restaurant_instant_discounts + sb.dining_discounts_given 
                                                            AS TOTAL_DINING_DISCOUNTS,
    
    ROUND(
        sb.movie_discounts_given + 
        sb.dining_discounts_given + 
        sb.snack_discounts_given + 
        sb.restaurant_instant_discounts,
        2
    )                                                       AS TOTAL_DISCOUNTS_GIVEN,
    
    ROUND(
        pp.PLAN_PRICE - (
            sb.movie_discounts_given + 
            sb.dining_discounts_given + 
            sb.snack_discounts_given + 
            sb.restaurant_instant_discounts
        ),
        2
    )                                                       AS NET_REVENUE_IMPACT,
    
    ROUND(
        ((sb.movie_discounts_given + 
          sb.dining_discounts_given + 
          sb.snack_discounts_given + 
          sb.restaurant_instant_discounts) / 
         NULLIF(pp.PLAN_PRICE, 0)) * 100,
        2
    )                                                       AS DISCOUNT_TO_REVENUE_RATIO,
    
    -- BENEFIT USAGE COUNTS
    sb.movie_benefits_used                                  AS MOVIE_BENEFITS_USED,
    sb.dining_benefits_used                                 AS DINING_BENEFITS_USED,
    sb.snack_benefits_used                                  AS SNACK_BENEFITS_USED,
    sb.movie_benefits_used + 
    sb.dining_benefits_used + 
    sb.snack_benefits_used                                  AS TOTAL_BENEFITS_REDEEMED,
    
    -- BENEFIT LIMITS (from plan)
    pp.MOVIE_BENEFIT_LIMIT,
    pp.DINING_BENEFIT_LIMIT,
    CASE WHEN pp.SNACK_IS_UNLIMITED THEN NULL ELSE 999 END  AS SNACK_BENEFIT_LIMIT,
    
    -- UTILIZATION PERCENTAGES
    ROUND(
        (sb.movie_benefits_used::FLOAT / 
         NULLIF(pp.MOVIE_BENEFIT_LIMIT, 0)) * 100,2) AS MOVIE_UTILIZATION_PCT,
    
    ROUND(
        (sb.dining_benefits_used::FLOAT / 
         NULLIF(pp.DINING_BENEFIT_LIMIT, 0)) * 100,2) AS DINING_UTILIZATION_PCT,
    
    ROUND(
        ((sb.movie_benefits_used + sb.dining_benefits_used)::FLOAT / 
         NULLIF(pp.MOVIE_BENEFIT_LIMIT + pp.DINING_BENEFIT_LIMIT, 0)) * 100,2) AS OVERALL_UTILIZATION_PCT,
    
    -- BOOKING METRICS
    sb.total_bookings                                       AS TOTAL_BOOKINGS,
    sb.movie_bookings                                       AS MOVIE_BOOKINGS,
    sb.dining_bookings                                      AS DINING_BOOKINGS,
    sb.snack_bookings                                       AS SNACK_BOOKINGS,
    
    sb.total_spend_during_pass                              AS TOTAL_SPEND_DURING_PASS,
    ROUND(sb.avg_booking_value, 2)                          AS AVG_BOOKING_VALUE,
    
    ROUND(
        sb.total_bookings::FLOAT / 
        NULLIF((pp.VALIDITY_DAYS / 7.0), 0),2) AS BOOKINGS_PER_WEEK,
    
    -- REDEMPTION TIMING
    DATEDIFF('day', sb.start_date, sb.first_redemption_date) 
                                                            AS DAYS_TO_FIRST_REDEMPTION,
    fbl.first_benefit_type                                  AS FIRST_BENEFIT_TYPE,
    sb.first_redemption_date                                AS FIRST_REDEMPTION_DATE,
    
    DATEDIFF('day', sb.start_date, sb.last_redemption_date) 
                                                            AS DAYS_TO_LAST_REDEMPTION,
    sb.last_redemption_date                                 AS LAST_REDEMPTION_DATE,
    
    DATEDIFF('day', sb.first_redemption_date, sb.last_redemption_date) 
                                                            AS REDEMPTION_SPAN_DAYS,
    
    -- CUSTOMER SEGMENTATION FLAGS
    CASE
        WHEN sb.movie_benefits_used >= pp.MOVIE_BENEFIT_LIMIT
             AND sb.dining_benefits_used >= pp.DINING_BENEFIT_LIMIT
        THEN 'Power User'
        WHEN (sb.movie_benefits_used + sb.dining_benefits_used + sb.snack_benefits_used) 
             BETWEEN 1 AND 2
        THEN 'Casual User'
        WHEN (sb.movie_benefits_used + sb.dining_benefits_used + sb.snack_benefits_used) = 0
        THEN 'Underutilizer'
        ELSE 'Regular User'
    END AS CUSTOMER_TYPE,
    
    CASE
        WHEN sb.movie_benefits_used >= pp.MOVIE_BENEFIT_LIMIT
             AND sb.dining_benefits_used >= pp.DINING_BENEFIT_LIMIT
        THEN TRUE ELSE FALSE
    END  AS IS_POWER_USER,
    
    CASE
        WHEN (sb.movie_benefits_used + sb.dining_benefits_used + sb.snack_benefits_used) 
             BETWEEN 1 AND 2
        THEN TRUE ELSE FALSE
    END  AS IS_CASUAL_USER,
    
    CASE
        WHEN (sb.movie_benefits_used + sb.dining_benefits_used + sb.snack_benefits_used) = 0
        THEN TRUE ELSE FALSE
    END  AS IS_UNDERUTILIZER,
    
    CASE
        WHEN (CASE WHEN sb.movie_benefits_used > 0 THEN 1 ELSE 0 END +
              CASE WHEN sb.dining_benefits_used > 0 THEN 1 ELSE 0 END +
              CASE WHEN sb.snack_benefits_used > 0 THEN 1 ELSE 0 END) >= 2
        THEN TRUE ELSE FALSE
    END  AS IS_MULTI_CATEGORY,
    
    -- ══════════════════════════════════════════════════════════
    -- PAYMENT & METADATA
    -- ══════════════════════════════════════════════════════════
    sb.payment_method                                       AS PAYMENT_METHOD,
    'Paid'                                                  AS PAYMENT_STATUS,
    'TXN_' || UPPER(LEFT(MD5(sb.subscription_id), 10))      AS TRANSACTION_ID,
    
    CURRENT_TIMESTAMP()                                     AS UPDATED_AT,
    sb.source_file                                          AS SOURCE_FILE

FROM subscription_base sb

-- Get first benefit type used (from separate CTE)
LEFT JOIN first_benefit_lookup fbl
    ON sb.subscription_id = fbl.PASS_SUBSCRIPTION_ID

-- Get plan attributes (price, validity, limits)
CROSS JOIN (
    SELECT 
        PASS_PLAN_KEY,
        PLAN_NAME,
        PLAN_PRICE,
        VALIDITY_DAYS,
        MOVIE_BENEFIT_LIMIT,
        DINING_BENEFIT_LIMIT,
        SNACK_IS_UNLIMITED
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN
    WHERE IS_CURRENT = TRUE
    LIMIT 1
) pp

-- Date dimension joins
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE d_pur 
    ON sb.purchase_date = d_pur.FULL_DATE
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE d_sta 
    ON sb.start_date = d_sta.FULL_DATE
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE d_end 
    ON DATEADD('day', pp.VALIDITY_DAYS, sb.start_date) = d_end.FULL_DATE;



alter dynamic table fact_pass_subscription refresh ; 
select * from fact_pass_subscription ;
