    -- =====================================================================================
    -- TABLE 2: gold_revenue_daily
    -- Purpose: Daily revenue trends for time-series analysis
    -- =====================================================================================
CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_revenue_daily
    TARGET_LAG = '25 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT 
    d.full_date AS booking_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_month,
    d.day_name,
    d.is_weekend,
    d.is_holiday,
COALESCE(SUM(f.total_amount), 0)        AS total_revenue,
COALESCE(round(AVG(f.total_amount),2), 0)        AS avg_booking_value,   -- or keep NULL for "no data" semantics
COALESCE(SUM(f.discount_applied), 0)    AS total_discounts,
COALESCE(SUM(f.convenience_fee), 0)     AS total_convenience_fees,

-- Cancellation rate → 0 when no bookings
    COUNT(DISTINCT f.booking_id) AS total_bookings,
    COUNT(DISTINCT f.customer_key) AS unique_customers,

  COUNT(DISTINCT CASE WHEN f.booking_category = 'Movies' THEN f.booking_id END) AS movie_bookings,
    COUNT(DISTINCT CASE WHEN f.booking_category in ('Dining','Food Festival') THEN f.booking_id END) AS dining_bookings,
    COUNT(DISTINCT CASE WHEN f.booking_category NOT IN ('Movies','Dining','Food Festival')  THEN f.booking_id END) AS event_bookings,
    
    SUM(CASE WHEN f.booking_category = 'Movies' THEN f.total_amount ELSE 0 END) AS movie_revenue,
    SUM(CASE WHEN f.booking_category in ('Dining','Food Festival') THEN f.total_amount ELSE 0 END) AS dining_revenue,
    SUM(CASE WHEN f.booking_category NOT IN  ('Movies','Dining','Food Festival')THEN f.total_amount ELSE 0 END) AS event_revenue,
    
    COUNT(DISTINCT CASE WHEN f.booking_status = 'CONFIRMED' THEN f.booking_id END) AS confirmed_bookings,
    COUNT(DISTINCT CASE WHEN f.is_cancelled = TRUE THEN f.booking_id END) AS cancelled_bookings,
    COALESCE(
    ROUND(
        COUNT(
            DISTINCT CASE WHEN f.is_cancelled = TRUE THEN f.booking_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT f.booking_id),0),
            2),0)  AS cancellation_rate_pct,
    
    COUNT(DISTINCT CASE WHEN f.payment_status = 'SUCCESS' THEN f.booking_id END) AS successful_payments,
    COUNT(DISTINCT CASE WHEN f.payment_status = 'FAILED' THEN f.booking_id END) AS failed_payments,
    -- Moving averages → coalesce the inner SUM
AVG(round(COALESCE(SUM(f.total_amount), 0),2)) OVER (
    ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)  AS revenue_7day_ma,
AVG(round(COALESCE(SUM(f.total_amount), 0),2)) OVER (
    ORDER BY d.full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
)  AS revenue_30day_ma,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM DISTRICT_ANALYTICS_DB.silver_layer.dim_date d
LEFT JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
    ON d.date_key = f.booking_date_key
WHERE d.full_date <= CURRENT_DATE() AND d.full_date >= (SELECT MIN(TO_DATE(booking_date)) 
  FROM DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings)
GROUP BY 
    d.full_date,d.year,d.quarter, d.month,
    d.month_name,d.week_of_year,d.day_of_month,
    d.day_name, d.is_weekend, d.is_holiday;

select * from gold_revenue_daily ;
alter dynamic table gold_revenue_daily refresh  ; 
