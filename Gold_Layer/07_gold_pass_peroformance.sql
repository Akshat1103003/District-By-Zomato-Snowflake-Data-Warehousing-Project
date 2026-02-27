-- ================================================================
-- 07 : GOLD_PASS_PERFORMANCE_MART - CORRECTED VERSION
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA GOLD_LAYER;


CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.GOLD_LAYER.GOLD_PASS_PERFORMANCE_MART (

    -- Customer Dimensions
    CUSTOMER_KEY,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_CITY,
   -- Subscription Dimensions
    SUBSCRIPTION_KEY,
    SUBSCRIPTION_ID,
    PASS_PLAN_NAME,
    PASS_PLAN_PRICE,
    PASS_PLAN_VALIDITY_DAYS,
    -- Temporal Dimensions
    PURCHASE_DATE,
    START_DATE,
    END_DATE,
    COHORT_MONTH,
    COHORT_QUARTER,
    PURCHASE_YEAR,
    PURCHASE_MONTH_NAME,
    -- Status
    SUBSCRIPTION_STATUS,
    IS_ACTIVE,
    IS_EXPIRED,
    DAYS_SINCE_PURCHASE,
    DAYS_UNTIL_EXPIRY,
    -- Revenue Metrics
    PASS_REVENUE,
    MOVIE_DISCOUNTS_GIVEN,
    DINING_DISCOUNTS_GIVEN,
    SNACK_DISCOUNTS_GIVEN,
    TOTAL_DISCOUNTS_GIVEN,
    NET_REVENUE_IMPACT,
    DISCOUNT_TO_REVENUE_RATIO,

    -- Benefit Usage Metrics
    MOVIE_BENEFITS_USED,
    DINING_BENEFITS_USED,
    SNACK_BENEFITS_USED,
    TOTAL_BENEFITS_REDEEMED,

    MOVIE_BENEFIT_LIMIT,
    DINING_BENEFIT_LIMIT,
    SNACK_BENEFIT_LIMIT,       
    MOVIE_UTILIZATION_PCT,       -- Capped at 100%
    DINING_UTILIZATION_PCT,      -- Capped at 100%
    OVERALL_UTILIZATION_PCT,     -- Capped at 100%

    -- Booking Behavior Metrics
    TOTAL_BOOKINGS,
    MOVIE_BOOKINGS,
    DINING_BOOKINGS,
    SNACK_BOOKINGS,

    TOTAL_SPEND_DURING_PASS,
    AVG_BOOKING_VALUE,
    BOOKINGS_PER_WEEK,           -- BOOKING_FREQUENCY

    -- Timing Metrics
    DAYS_TO_FIRST_REDEMPTION,
    FIRST_BENEFIT_TYPE,
    FIRST_REDEMPTION_DATE,
    DAYS_TO_LAST_REDEMPTION,
    LAST_REDEMPTION_DATE,
    REDEMPTION_SPAN_DAYS,

    -- Customer Segment Flags
    CUSTOMER_TYPE,
    IS_POWER_USER,
    IS_CASUAL_USER,
    IS_UNDERUTILIZER,
    IS_MULTI_CATEGORY,

    -- Metadata
    UPDATED_AT
)
TARGET_LAG = '30 minutes'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = COMPUTE_WH
COMMENT = 'Gold layer data mart for District Pass performance analytics. Grain: One row per subscription.'
AS
WITH pass_subscriptions AS (
    SELECT
        ps.SUBSCRIPTION_KEY,
        ps.SUBSCRIPTION_ID,
        ps.CUSTOMER_KEY,
        ps.PASS_PLAN_KEY,
        ps.PURCHASE_DATE,
        ps.START_DATE,
        ps.END_DATE,
        ps.pass_plan_price ,
        ps.MOVIE_BENEFITS_USED,
        ps.DINING_BENEFITS_USED,
        ps.SNACK_BENEFITS_USED,

        -- Customer info
        c.CUSTOMER_ID AS cust_id,
        c.CUSTOMER_NAME,
        c.customer_city AS customer_city,

        -- Plan info
        pp.PLAN_NAME,
        pp.PLAN_PRICE,
        pp.VALIDITY_DAYS,
        pp.MOVIE_BENEFIT_LIMIT,
        pp.DINING_BENEFIT_LIMIT

    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_PASS_SUBSCRIPTION ps
    JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER c
        ON ps.CUSTOMER_KEY = c.CUSTOMER_KEY AND c.IS_CURRENT = TRUE
    JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN pp
        ON ps.PASS_PLAN_KEY = pp.PASS_PLAN_KEY
),
first_redemptions AS (
    SELECT
        br.SUBSCRIPTION_KEY,
        MIN(br.REDEMPTION_DATE) AS first_redemption_date
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BENEFIT_REDEMPTION br
    GROUP BY br.SUBSCRIPTION_KEY
),
benefit_aggregates AS (
    SELECT
        br.SUBSCRIPTION_KEY,
        SUM(CASE WHEN bt.BENEFIT_CATEGORY = 'Movie' THEN br.DISCOUNT_AMOUNT ELSE 0 END) AS movie_discounts,
        SUM(CASE WHEN bt.BENEFIT_CATEGORY = 'Dining' THEN br.DISCOUNT_AMOUNT ELSE 0 END) AS dining_discounts,
        SUM(CASE WHEN bt.BENEFIT_CATEGORY = 'Snack' THEN br.DISCOUNT_AMOUNT ELSE 0 END) AS snack_discounts,
        SUM(br.DISCOUNT_AMOUNT) AS total_discounts,
        MAX(br.REDEMPTION_DATE) AS last_redemption_date,
        MAX(CASE WHEN bt.BENEFIT_CATEGORY = 'Movie' THEN 1 ELSE 0 END) AS has_movie_redemptions,
        MAX(CASE WHEN bt.BENEFIT_CATEGORY = 'Dining' THEN 1 ELSE 0 END) AS has_dining_redemptions,
        MAX(CASE WHEN bt.BENEFIT_CATEGORY = 'Snack' THEN 1 ELSE 0 END) AS has_snack_redemptions
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BENEFIT_REDEMPTION br
    JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE bt
        ON br.BENEFIT_TYPE_KEY = bt.BENEFIT_TYPE_KEY
    GROUP BY br.SUBSCRIPTION_KEY
),
first_benefit_type_cte AS (
    SELECT
        br.SUBSCRIPTION_KEY,
        bt.BENEFIT_CATEGORY AS first_benefit_type
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BENEFIT_REDEMPTION br
    JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE bt
        ON br.BENEFIT_TYPE_KEY = bt.BENEFIT_TYPE_KEY
    JOIN first_redemptions fr
        ON br.SUBSCRIPTION_KEY = fr.SUBSCRIPTION_KEY
        AND br.REDEMPTION_DATE = fr.first_redemption_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY br.SUBSCRIPTION_KEY ORDER BY br.REDEMPTION_KEY) = 1
),
booking_aggregates AS (
    SELECT
        fb.PASS_SUBSCRIPTION_ID AS subscription_id,
        COUNT(DISTINCT fb.BOOKING_ID) AS total_bookings,
        COUNT(DISTINCT CASE WHEN fb.BOOKING_CATEGORY = 'Movies' THEN fb.BOOKING_ID END) AS movie_bookings,
        COUNT(DISTINCT CASE WHEN fb.BOOKING_CATEGORY = 'Dining' THEN fb.BOOKING_ID END) AS dining_bookings,
        COUNT(DISTINCT CASE WHEN fb.BOOKING_CATEGORY = 'Movies - Snacks' THEN fb.BOOKING_ID END) AS snack_bookings,
        SUM(fb.TOTAL_AMOUNT) AS total_spend,
        AVG(fb.TOTAL_AMOUNT) AS avg_booking_value
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BOOKINGS fb
    WHERE fb.PASS_SUBSCRIPTION_ID IS NOT NULL
    GROUP BY fb.PASS_SUBSCRIPTION_ID
)
SELECT
    -- Customer Dimensions
    ps.CUSTOMER_KEY,
    ps.cust_id AS CUSTOMER_ID,
    ps.CUSTOMER_NAME,
    ps.customer_city AS CUSTOMER_CITY,

    -- Subscription Dimensions
    ps.SUBSCRIPTION_KEY,
    ps.SUBSCRIPTION_ID,
    ps.PLAN_NAME AS PASS_PLAN_NAME,
    ps.PLAN_PRICE AS PASS_PLAN_PRICE,
    ps.VALIDITY_DAYS AS PASS_PLAN_VALIDITY_DAYS,

    -- Temporal Dimensions
    ps.PURCHASE_DATE,
    ps.START_DATE,
    ps.END_DATE,
    DATE_TRUNC('MONTH', ps.PURCHASE_DATE) AS COHORT_MONTH,
    DATE_TRUNC('QUARTER', ps.PURCHASE_DATE) AS COHORT_QUARTER,
    YEAR(ps.PURCHASE_DATE) AS PURCHASE_YEAR,
    MONTHNAME(ps.PURCHASE_DATE) AS PURCHASE_MONTH_NAME,

    -- Status
    CASE
        WHEN CURRENT_DATE() BETWEEN ps.START_DATE AND ps.END_DATE THEN 'Active'
        WHEN CURRENT_DATE() > ps.END_DATE THEN 'Expired'
        ELSE 'Future'
    END AS SUBSCRIPTION_STATUS,
    CASE WHEN CURRENT_DATE() BETWEEN ps.START_DATE AND ps.END_DATE THEN TRUE ELSE FALSE END AS IS_ACTIVE,
    CASE WHEN CURRENT_DATE() > ps.END_DATE THEN TRUE ELSE FALSE END AS IS_EXPIRED,
    DATEDIFF('day', ps.PURCHASE_DATE, CURRENT_DATE()) AS DAYS_SINCE_PURCHASE,

    -- FIX 5: DAYS_UNTIL_EXPIRY floored at 0 (no negative values in dashboard)
    GREATEST(DATEDIFF('day', CURRENT_DATE(), ps.END_DATE), 0) AS DAYS_UNTIL_EXPIRY,

    -- Revenue Metrics
    ps.pass_plan_price AS PASS_REVENUE,
    COALESCE(ba.movie_discounts, 0) AS MOVIE_DISCOUNTS_GIVEN,
    COALESCE(ba.dining_discounts, 0) AS DINING_DISCOUNTS_GIVEN,
    COALESCE(ba.snack_discounts, 0) AS SNACK_DISCOUNTS_GIVEN,
    COALESCE(ba.total_discounts, 0) AS TOTAL_DISCOUNTS_GIVEN,
    ps.pass_plan_price - COALESCE(ba.total_discounts, 0) AS NET_REVENUE_IMPACT,

    ROUND((COALESCE(ba.total_discounts, 0) * 100.0) / NULLIF(ps.pass_plan_price, 0), 2) AS DISCOUNT_TO_REVENUE_RATIO,

    -- Benefit Usage Metrics
    ps.MOVIE_BENEFITS_USED,
    ps.DINING_BENEFITS_USED,
    ps.SNACK_BENEFITS_USED,
    ps.MOVIE_BENEFITS_USED + ps.DINING_BENEFITS_USED + ps.SNACK_BENEFITS_USED AS TOTAL_BENEFITS_REDEEMED,

    ps.MOVIE_BENEFIT_LIMIT,
    ps.DINING_BENEFIT_LIMIT,

    NULL AS SNACK_BENEFIT_LIMIT,

    LEAST(
        ROUND((ps.MOVIE_BENEFITS_USED * 100.0) / NULLIF(ps.MOVIE_BENEFIT_LIMIT, 0), 2),
        100.00
    ) AS MOVIE_UTILIZATION_PCT,

    LEAST(
        ROUND((ps.DINING_BENEFITS_USED * 100.0) / NULLIF(ps.DINING_BENEFIT_LIMIT, 0), 2),
        100.00
    ) AS DINING_UTILIZATION_PCT,

    LEAST(
        ROUND(((ps.MOVIE_BENEFITS_USED + ps.DINING_BENEFITS_USED) * 100.0) /
              NULLIF(ps.MOVIE_BENEFIT_LIMIT + ps.DINING_BENEFIT_LIMIT, 0), 2),
        100.00
    ) AS OVERALL_UTILIZATION_PCT,

    -- Booking Behavior Metrics
    COALESCE(bka.total_bookings, 0) AS TOTAL_BOOKINGS,
    COALESCE(bka.movie_bookings, 0) AS MOVIE_BOOKINGS,
    COALESCE(bka.dining_bookings, 0) AS DINING_BOOKINGS,
    COALESCE(bka.snack_bookings, 0) AS SNACK_BOOKINGS,
    COALESCE(bka.total_spend, 0) AS TOTAL_SPEND_DURING_PASS,
    COALESCE(bka.avg_booking_value, 0) AS AVG_BOOKING_VALUE,

    ROUND(
        COALESCE(bka.total_bookings, 0) * 7.0 /
        NULLIF(DATEDIFF('day', ps.START_DATE, LEAST(ps.END_DATE, CURRENT_DATE())), 0),
        2
    ) AS BOOKINGS_PER_WEEK,

    -- Timing Metrics
    DATEDIFF('day', ps.PURCHASE_DATE, fr.first_redemption_date) AS DAYS_TO_FIRST_REDEMPTION,
    fbt.first_benefit_type AS FIRST_BENEFIT_TYPE,
    fr.first_redemption_date AS FIRST_REDEMPTION_DATE,
    DATEDIFF('day', ps.PURCHASE_DATE, ba.last_redemption_date) AS DAYS_TO_LAST_REDEMPTION,
    ba.last_redemption_date AS LAST_REDEMPTION_DATE,
    DATEDIFF('day', fr.first_redemption_date, ba.last_redemption_date) AS REDEMPTION_SPAN_DAYS,

    -- Customer Segment Flags
    CASE
        WHEN ps.MOVIE_BENEFITS_USED >= ps.MOVIE_BENEFIT_LIMIT
             AND ps.DINING_BENEFITS_USED >= ps.DINING_BENEFIT_LIMIT THEN 'Power User'
        WHEN (ps.MOVIE_BENEFITS_USED + ps.DINING_BENEFITS_USED) >=
             ((ps.MOVIE_BENEFIT_LIMIT + ps.DINING_BENEFIT_LIMIT) * 0.5) THEN 'Casual User'
        ELSE 'Underutilizer'
    END AS CUSTOMER_TYPE,

    CASE WHEN ps.MOVIE_BENEFITS_USED >= ps.MOVIE_BENEFIT_LIMIT
              AND ps.DINING_BENEFITS_USED >= ps.DINING_BENEFIT_LIMIT
         THEN TRUE ELSE FALSE END AS IS_POWER_USER,

    CASE WHEN (ps.MOVIE_BENEFITS_USED + ps.DINING_BENEFITS_USED) BETWEEN
              ((ps.MOVIE_BENEFIT_LIMIT + ps.DINING_BENEFIT_LIMIT) * 0.5)
              AND ((ps.MOVIE_BENEFIT_LIMIT + ps.DINING_BENEFIT_LIMIT) * 0.8)
         THEN TRUE ELSE FALSE END AS IS_CASUAL_USER,

    CASE WHEN (ps.MOVIE_BENEFITS_USED + ps.DINING_BENEFITS_USED) <
              ((ps.MOVIE_BENEFIT_LIMIT + ps.DINING_BENEFIT_LIMIT) * 0.5)
         THEN TRUE ELSE FALSE END AS IS_UNDERUTILIZER,

    CASE WHEN (COALESCE(ba.has_movie_redemptions, 0) +
               COALESCE(ba.has_dining_redemptions, 0) +
               COALESCE(ba.has_snack_redemptions, 0)) >= 2
         THEN TRUE ELSE FALSE END AS IS_MULTI_CATEGORY,

    -- Metadata
    CURRENT_TIMESTAMP() AS UPDATED_AT

FROM pass_subscriptions ps
LEFT JOIN first_redemptions fr ON ps.SUBSCRIPTION_KEY = fr.SUBSCRIPTION_KEY
LEFT JOIN first_benefit_type_cte fbt ON ps.SUBSCRIPTION_KEY = fbt.SUBSCRIPTION_KEY
LEFT JOIN benefit_aggregates ba ON ps.SUBSCRIPTION_KEY = ba.SUBSCRIPTION_KEY
LEFT JOIN booking_aggregates bka ON ps.SUBSCRIPTION_ID = bka.subscription_id;

COMMENT ON TABLE DISTRICT_ANALYTICS_DB.GOLD_LAYER.GOLD_PASS_PERFORMANCE_MART
    IS 'Pre-aggregated Pass performance metrics for Power BI dashboards. Refreshes every 30 minutes from Silver layer.';

SHOW DYNAMIC TABLES LIKE 'GOLD_PASS_PERFORMANCE_MART' IN SCHEMA GOLD_LAYER;

alter dynamic table gold_pass_performance_mart refresh  ;
select * from gold_layer.gold_pass_performance_mart ;
