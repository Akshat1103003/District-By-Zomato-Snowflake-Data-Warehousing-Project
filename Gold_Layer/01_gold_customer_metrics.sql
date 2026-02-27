USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA gold_layer;
-- =====================================================================================
-- TABLE 1: gold_customer_metrics
-- Purpose: Customer segmentation, behavior, and lifetime value analysis
-- ==================================================================
CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_customer_metrics
TARGET_LAG = '25 minute' 
WAREHOUSE = COMPUTE_WH 
COMMENT = 'Customer analytics: segmentation, behavior, and lifetime value metrics' 
AS
SELECT
    -- Customer identifiers
    c.customer_key,
    c.customer_id, 
    c.customer_name,
    
    -- Demographics
    c.customer_gender,
    c.age_group, 
    c.customer_city, 
    c.user_state,
    c.user_country, 
    c.loyalty_tier, 
    c.USER_SIGNUP_DATE,
    
    -- Booking metrics
    COUNT(DISTINCT f.booking_id) AS total_bookings,
    
    -- Movie bookings (Movies + Movies - Snacks)
    COUNT(DISTINCT CASE
        WHEN f.booking_category = 'Movies' THEN f.booking_id END) AS movie_bookings,
    
    -- Dining bookings (Dining + Food Festival)
    COUNT(DISTINCT CASE 
        WHEN f.booking_category IN ('Dining','Food Festival') THEN f.booking_id END) AS dining_bookings,
    
    -- Event bookings (everything else)
    COUNT(DISTINCT CASE 
        WHEN f.booking_category NOT IN ('Movies','Dining','Food Festival') THEN f.booking_id END) AS event_bookings,
    
    -- Revenue metrics
    SUM(f.total_amount) AS lifetime_revenue,
    AVG(f.total_amount) AS avg_booking_value,
    MAX(f.total_amount) AS max_booking_value,
    MIN(f.total_amount) AS min_booking_value,
    
    -- Discount metrics
    SUM(f.discount_applied) AS total_discounts_received,
    AVG(f.discount_applied) AS avg_discount_per_booking,
    ROUND(SUM(f.discount_applied) * 100.0 / NULLIF(SUM(f.total_amount + f.discount_applied), 0),2) AS discount_rate_pct,
    
    -- Engagement metrics
    MIN(f.booking_date) AS first_booking_date,
    MAX(f.booking_date) AS last_booking_date,
    DATEDIFF('day', MIN(f.booking_date), MAX(f.booking_date)) AS customer_tenure_days,
    DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) AS days_since_last_booking,
    
    -- Booking frequency
    COUNT(DISTINCT f.booking_date) AS unique_booking_days,
    ROUND(
        COUNT(DISTINCT f.booking_id) * 1.0 / NULLIF(DATEDIFF('day', MIN(f.booking_date), MAX(f.booking_date)),0)
    ,2) AS bookings_per_day,
    
    -- Cancellation metrics
    COUNT(DISTINCT CASE
        WHEN f.is_cancelled = TRUE THEN f.booking_id END) AS cancelled_bookings,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN f.is_cancelled = TRUE THEN f.booking_id 
        END) * 100.0 / NULLIF(COUNT(DISTINCT f.booking_id), 0),
        2) AS cancellation_rate_pct,
    
    -- Review metrics
    AVG(f.rating) AS avg_rating_given,
    COUNT(DISTINCT CASE
        WHEN f.rating IS NOT NULL THEN f.booking_id END) AS bookings_with_reviews,
    
    -- RFM scoring
    CASE
        WHEN DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) <= 90 THEN 'At Risk'
        WHEN DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) <= 180 THEN 'Dormant'
        ELSE 'Churned'
    END AS customer_status,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS last_updated

FROM DISTRICT_ANALYTICS_DB.silver_layer.dim_customer c
LEFT JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
    ON c.customer_key = f.customer_key
WHERE c.is_current = TRUE
GROUP BY
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.customer_gender,
    c.age_group,
    c.CUSTOMER_CITY,
    c.user_state,
    c.user_country,
    c.loyalty_tier,
    c.user_signup_date;

SELECT '✅ CREATED : gold_customer_metrics ' AS status;

alter dynamic table gold_customer_metrics refresh ;
select * from gold_customer_metrics  ;
