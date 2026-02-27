-- =====================================================================================
    -- TABLE 5: gold_booking_trends
    -- Purpose: Booking patterns analysis (by time, day, category)
    -- =====================================================================================
CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_booking_trends
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Booking trends aggregation.'
AS
SELECT 
    d.year, d.month, d.month_name, d.week_of_year , d.day_name, d.is_weekend,
    
    CASE 
        WHEN f.booking_category = 'Movies' THEN 'Movies'
        WHEN f.booking_category IN ('Dining', 'Food Festival') THEN 'Dining'
        ELSE 'Events'
    END AS booking_category,
    
    c.customer_gender , c.age_group, c.loyalty_tier, c.customer_city,
    
    COUNT(DISTINCT f.booking_id) AS total_bookings,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    SUM(f.total_amount) AS total_revenue,
    round(AVG(f.total_amount),2) AS avg_booking_value,
    SUM(f.quantity) AS total_tickets,
    ROUND(COUNT(DISTINCT f.booking_id) * 1.0 / NULLIF(COUNT(DISTINCT f.customer_key), 0), 2) AS bookings_per_customer,
    
    CURRENT_TIMESTAMP() AS last_updated
FROM DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f
INNER JOIN DISTRICT_ANALYTICS_DB.silver_layer.dim_date d 
    ON f.booking_date = d.full_date  -- Join on natural date instead of surrogate key
INNER JOIN DISTRICT_ANALYTICS_DB.silver_layer.dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY 
    d.year, d.month, d.month_name, d.week_of_year, d.day_name, d.is_weekend,
    CASE 
        WHEN f.booking_category = 'Movies' THEN 'Movies'
        WHEN f.booking_category IN ('Dining', 'Food Festival') THEN 'Dining'
        ELSE 'Events'
    END,
    c.customer_gender, c.age_group, c.loyalty_tier, c.customer_city;

select *  from gold_booking_trends  ;
alter dynamic table gold_booking_trends refresh ;
