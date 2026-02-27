
USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- Drop & recreate
DROP TABLE IF EXISTS DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE;

CREATE OR REPLACE TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE (

    -- Primary key: YYYYMMDD format (e.g. 20250115)
    -- Readable + joins correctly with FACT_BOOKINGS date key derivation
    DATE_KEY            INTEGER         NOT NULL PRIMARY KEY,

    FULL_DATE           DATE            NOT NULL UNIQUE,

    -- Day level
    DAY_OF_MONTH        INTEGER         NOT NULL,
    DAY_OF_WEEK         INTEGER         NOT NULL   COMMENT '0=Sunday, 6=Saturday',
    DAY_NAME            VARCHAR(20)     NOT NULL   COMMENT 'Monday, Tuesday...',
    IS_WEEKEND          BOOLEAN         NOT NULL,

    -- Week level
    WEEK_OF_YEAR        INTEGER         NOT NULL,

    -- Month level
    MONTH               INTEGER         NOT NULL,
    MONTH_NAME          VARCHAR(20)     NOT NULL   COMMENT 'January, February...',
    MONTH_SHORT         VARCHAR(10)     NOT NULL   COMMENT 'Jan, Feb...',
    MONTH_YEAR          VARCHAR(10)     NOT NULL   COMMENT 'Jan-2025, Feb-2025...',

    -- Quarter level
    QUARTER             INTEGER         NOT NULL   COMMENT '1, 2, 3, 4',
    QUARTER_NAME        VARCHAR(5)      NOT NULL   COMMENT 'Q1, Q2, Q3, Q4',
    QUARTER_YEAR        VARCHAR(10)     NOT NULL   COMMENT 'Q1-2025, Q2-2025...',  -- ← NEW

    -- Year level
    YEAR                INTEGER         NOT NULL,

    -- Fiscal year (India: April–March)
    FISCAL_YEAR         INTEGER         NOT NULL   COMMENT 'FY2025 = Apr2024-Mar2025',
    FISCAL_QUARTER      INTEGER         NOT NULL   COMMENT 'Q1=Apr-Jun, Q2=Jul-Sep...',
    FISCAL_QUARTER_YEAR VARCHAR(10)     NOT NULL   COMMENT 'FQ1-FY2025',

    -- Holiday flag (extendable)
    IS_HOLIDAY          BOOLEAN         NOT NULL DEFAULT FALSE,
    HOLIDAY_NAME        VARCHAR(100),

    -- Metadata
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Date dimension. SCD Type 0 (static). DATE_KEY = YYYYMMDD. Covers 2020-2030.';

-- ================================================================
-- POPULATE: 2020-01-01 to 2030-12-31 (4,018 rows)
-- ================================================================

INSERT INTO DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE (
    DATE_KEY,
    FULL_DATE,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    DAY_NAME,
    IS_WEEKEND,
    WEEK_OF_YEAR,
    MONTH,
    MONTH_NAME,
    MONTH_SHORT,
    MONTH_YEAR,
    QUARTER,
    QUARTER_NAME,
    QUARTER_YEAR,
    YEAR,
    FISCAL_YEAR,
    FISCAL_QUARTER,
    FISCAL_QUARTER_YEAR,
    IS_HOLIDAY,
    HOLIDAY_NAME
)
WITH date_spine AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2025-01-01')::DATE AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
    WHERE DATEADD(DAY, SEQ4(), '2025-01-01')::DATE <= '2030-12-31'
)
SELECT
    -- DATE_KEY: YYYYMMDD
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))                           AS DATE_KEY,
    d                                                           AS FULL_DATE,

    -- Day
    DAYOFMONTH(d)                                               AS DAY_OF_MONTH,
    DAYOFWEEK(d)                                                AS DAY_OF_WEEK,
    DAYNAME(d)                                                  AS DAY_NAME,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END   AS IS_WEEKEND,

    -- Week
    WEEKOFYEAR(d)                                               AS WEEK_OF_YEAR,

    -- Month
    MONTH(d)                                                    AS MONTH,
    MONTHNAME(d)                                                AS MONTH_NAME,
    LEFT(MONTHNAME(d), 3)                                       AS MONTH_SHORT,
    LEFT(MONTHNAME(d), 3) || '-' || YEAR(d)::VARCHAR            AS MONTH_YEAR,

    -- Quarter
    QUARTER(d)                                                  AS QUARTER,
    'Q' || QUARTER(d)::VARCHAR                                  AS QUARTER_NAME,
    'Q' || QUARTER(d)::VARCHAR || '-' || YEAR(d)::VARCHAR       AS QUARTER_YEAR,

    -- Year
    YEAR(d)                                                     AS YEAR,

    -- Fiscal Year (India: April = start of new FY)
    CASE
        WHEN MONTH(d) >= 4 THEN YEAR(d)
        ELSE YEAR(d) - 1
    END                                                         AS FISCAL_YEAR,

    CASE
        WHEN MONTH(d) IN (4,5,6)   THEN 1   -- Apr-Jun  = FQ1
        WHEN MONTH(d) IN (7,8,9)   THEN 2   -- Jul-Sep  = FQ2
        WHEN MONTH(d) IN (10,11,12) THEN 3  -- Oct-Dec  = FQ3
        ELSE 4                               -- Jan-Mar  = FQ4
    END                                                         AS FISCAL_QUARTER,

    'FQ' ||
    CASE
        WHEN MONTH(d) IN (4,5,6)    THEN '1'
        WHEN MONTH(d) IN (7,8,9)    THEN '2'
        WHEN MONTH(d) IN (10,11,12) THEN '3'
        ELSE '4'
    END
    || '-FY' ||
    CASE
        WHEN MONTH(d) >= 4 THEN YEAR(d)::VARCHAR
        ELSE (YEAR(d) - 1)::VARCHAR
    END                                                         AS FISCAL_QUARTER_YEAR,

    -- Holidays (FALSE by default - update selectively below)
    FALSE                                                       AS IS_HOLIDAY,
    NULL                                                        AS HOLIDAY_NAME

FROM date_spine;

-- ================================================================
-- MARK MAJOR INDIAN PUBLIC HOLIDAYS
-- Covers 2025 and 2026 as examples - extend as needed
-- ================================================================

UPDATE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE
SET IS_HOLIDAY = TRUE, HOLIDAY_NAME = holiday
FROM (VALUES
    -- 2025
    ('2025-01-26', 'Republic Day'),
    ('2025-03-14', 'Holi'),
    ('2025-04-14', 'Dr. Ambedkar Jayanti'),
    ('2025-04-18', 'Good Friday'),
    ('2025-05-12', 'Buddha Purnima'),
    ('2025-08-15', 'Independence Day'),
    ('2025-08-27', 'Janmashtami'),
    ('2025-10-02', 'Gandhi Jayanti'),
    ('2025-10-20', 'Dussehra'),
    ('2025-10-20', 'Diwali'),
    ('2025-11-05', 'Diwali'),
    ('2025-12-25', 'Christmas'),
    -- 2026
    ('2026-01-26', 'Republic Day'),
    ('2026-03-03', 'Holi'),
    ('2026-04-03', 'Good Friday'),
    ('2026-08-15', 'Independence Day'),
    ('2026-10-02', 'Gandhi Jayanti'),
    ('2026-10-19', 'Dussehra'),
    ('2026-11-08', 'Diwali'),
    ('2026-12-25', 'Christmas')
) AS holidays(dt, holiday)
WHERE FULL_DATE = TO_DATE(dt);

select * from dim_date ;
