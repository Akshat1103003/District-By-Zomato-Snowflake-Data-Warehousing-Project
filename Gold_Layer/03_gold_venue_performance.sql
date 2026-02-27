-- ===================================================================================
    -- TABLE 3: gold_venue_performance
    -- Purpose: Venue popularity, capacity utilization, and revenue analysis
    -- ===============================================================================
CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_venue_performance 
TARGET_LAG = '25 minute' 
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = COMPUTE_WH  
COMMENT = 'Venue performance metrics: popularity, capacity utilization, revenue. One row per venue (VENUE_ID).'
AS
SELECT
    -- VENUE IDENTIFIERS
    v.venue_key,
    v.venue_id,
    v.venue_name,
    v.venue_type,
    v.venue_city,
    v.venue_state,
    v.venue_country,
    v.venue_categories,
    v.venue_capacity,
    v.total_screens,
    v.seating_sections,
    v.latitude,
    v.longitude,
    v.venue_rating,
    v.total_reviews AS venue_total_reviews,
    -- BOOKING VOLUME METRICS
    COUNT(DISTINCT f.booking_id) AS total_bookings,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(DISTINCT f.booking_date) AS days_with_bookings,
    
    -- Bookings per customer (engagement)
    ROUND(
        COUNT(DISTINCT f.booking_id) * 1.0 / 
        NULLIF(COUNT(DISTINCT f.customer_key), 0),2) AS bookings_per_customer,
    -- REVENUE METRICS
    
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_booking_value,
  
    ROUND( SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.booking_id), 0),2) AS revenue_per_booking,
    -- CAPACITY METRICS
    SUM(f.quantity) AS total_tickets_sold,
    
    -- Average tickets per booking
    ROUND(
        SUM(f.quantity) * 1.0 / NULLIF(COUNT(DISTINCT f.booking_id), 0), 2 ) AS avg_tickets_per_booking,
    
    -- Capacity utilization (capped at 100%)
    LEAST(CASE
            WHEN v.venue_capacity IS NOT NULL 
            AND v.venue_capacity > 0 
            AND COUNT(DISTINCT f.booking_date) > 0
            THEN ROUND(
                SUM(f.quantity) * 100.0 / (v.venue_capacity * COUNT(DISTINCT f.booking_date)), 2)
            ELSE NULL 
        END,100.00)
    AS avg_capacity_utilization_pct,
    -- CATEGORY BREAKDOWN
    COUNT(DISTINCT CASE 
        WHEN f.booking_category = 'Movies' THEN f.booking_id 
    END) AS movie_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN f.booking_category IN ('Dining', 'Food Festival') THEN f.booking_id 
    END) AS dining_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN f.booking_category NOT IN ('Movies', 'Dining', 'Food Festival') THEN f.booking_id 
    END) AS event_bookings,
    
    SUM(CASE 
        WHEN f.booking_category = 'Movies' THEN f.total_amount ELSE 0 
    END) AS movie_revenue,
    
    SUM(CASE 
        WHEN f.booking_category IN ('Dining', 'Food Festival') THEN f.total_amount ELSE 0 
    END) AS dining_revenue,
    
    SUM(CASE 
        WHEN f.booking_category NOT IN ('Movies', 'Dining', 'Food Festival') THEN f.total_amount ELSE 0 
    END) AS event_revenue,
    
    -- TIMING ANALYSIS
    MIN(f.booking_date) AS first_booking_date,
    MAX(f.booking_date) AS last_booking_date,
    DATEDIFF('day', MIN(f.booking_date), MAX(f.booking_date)) AS days_active,
    
    -- Recency
    DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) AS days_since_last_booking,
    
-- CUSTOMER SATISFACTION
    ROUND(AVG(f.rating), 2) AS avg_customer_rating,
    COUNT(DISTINCT CASE 
        WHEN f.rating IS NOT NULL THEN f.booking_id 
    END) AS bookings_with_ratings,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN f.rating >= 4 THEN f.booking_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN f.rating IS NOT NULL THEN f.booking_id END), 0),
        2
    ) AS high_rating_pct,

    -- CANCELLATION METRICS
    COUNT(DISTINCT CASE 
        WHEN f.is_cancelled = TRUE THEN f.booking_id 
    END) AS cancelled_bookings,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN f.is_cancelled = TRUE THEN f.booking_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT f.booking_id), 0), 
        2
    ) AS cancellation_rate_pct,
    
    -- PASS METRICS (Pass subscriber activity)
    COUNT(DISTINCT CASE 
        WHEN f.has_pass_benefit = TRUE THEN f.booking_id 
    END) AS pass_bookings,
    
    SUM(CASE 
        WHEN f.has_pass_benefit = TRUE THEN f.total_amount ELSE 0 
    END) AS pass_revenue,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN f.has_pass_benefit = TRUE THEN f.booking_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT f.booking_id), 0),
    2) AS pass_penetration_pct,
    
    -- VENUE STATUS
    CASE
        WHEN COUNT(DISTINCT f.booking_id) = 0 THEN 'No Bookings'
        WHEN DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', MAX(f.booking_date), CURRENT_DATE()) <= 90 THEN 'Low Activity'
        ELSE 'Inactive'
    END AS venue_status,
    -- METADATA
    CURRENT_TIMESTAMP() AS last_updated
    
FROM DISTRICT_ANALYTICS_DB.silver_layer.dim_venue v
LEFT JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
    ON v.venue_key = f.venue_key

GROUP BY
    v.venue_key, v.venue_id, v.venue_name,
    v.venue_type, v.venue_city, v.venue_state,
    v.venue_country, v.venue_categories, v.venue_capacity,
    v.total_screens, v.seating_sections, v.latitude,
    v.longitude, v.venue_rating, v.total_reviews;

SELECT '✅ gold_venue_performance created successfully!' AS status;

alter dynamic table gold_venue_performance refresh ;
select * from gold_venue_performance ;
