    -- ================================================================
    -- TABLE 4: gold_movie_analytics
    -- Purpose: Movie performance, popularity, and revenue analysis
    -- ================================================================

CREATE OR REPLACE DYNAMIC TABLE gold_movie_analytics
    TARGET_LAG = '25 minute' 
    WAREHOUSE = COMPUTE_WH 
    COMMENT = 'Movie performance aggregated by title (handles duplicate EVENT_IDs)'
AS
SELECT
    MIN(m.movie_id) AS movie_id,  
    
    ANY_VALUE(m.movie_title) AS movie_title,
    ANY_VALUE(m.language) AS language,
    ANY_VALUE(m.genre) AS genre,
    ANY_VALUE(m.rating) AS movie_certification,
    ANY_VALUE(m.duration_minutes) AS duration_minutes,
    
    COUNT(DISTINCT f.booking_id) AS total_bookings,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.quantity) AS total_tickets_sold,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_booking_value,
    MAX(f.total_amount) AS max_booking_value,
    
    COUNT(DISTINCT CASE WHEN c.customer_gender = 'Male' THEN f.booking_id END) AS male_bookings,
    COUNT(DISTINCT CASE WHEN c.customer_gender = 'Female' THEN f.booking_id END) AS female_bookings,
    COUNT(DISTINCT CASE WHEN c.age_group = '18-25' THEN f.booking_id END) AS age_18_25_bookings,
    COUNT(DISTINCT CASE WHEN c.age_group = '26-35' THEN f.booking_id END) AS age_26_35_bookings,
    COUNT(DISTINCT CASE WHEN c.age_group = '36-45' THEN f.booking_id END) AS age_36_45_bookings,
    COUNT(DISTINCT c.customer_city ) AS cities_with_bookings,
    
    ROUND(AVG(f.rating), 1) AS avg_customer_rating,
    COUNT(DISTINCT CASE WHEN f.rating >= 4 THEN f.booking_id END) AS highly_rated_bookings,
    ROUND(
        COUNT(DISTINCT CASE WHEN f.rating >= 4 THEN f.booking_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN f.rating IS NOT NULL THEN f.booking_id END), 0), 2
    ) AS high_rating_pct,
    
    MIN(f.booking_date) AS first_booking_date,
    MAX(f.booking_date) AS last_booking_date,
    DATEDIFF('day', MIN(f.booking_date), MAX(f.booking_date)) AS days_in_theaters,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM DISTRICT_ANALYTICS_DB.silver_layer.dim_movie m 
INNER JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
    ON m.movie_id = f.movie_id 
    AND f.booking_category = 'Movies'
LEFT JOIN DISTRICT_ANALYTICS_DB.silver_layer.dim_customer c 
    ON f.customer_key = c.customer_key 
    AND c.is_current = TRUE
GROUP BY m.movie_title, m.language, m.genre, m.rating, m.duration_minutes;

alter dynamic table gold_movie_analytics refresh ;
select * from gold_movie_analytics ;
