-- ================================================================
-- GOLD_PASS_DAILY_KPIS - Daily Pass Performance Metrics
-- ================================================================
-- Execute Date: 2026-02-19
-- Purpose: Daily Pass analytics for time-series dashboards
-- Grain: One row per day
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA GOLD_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- CREATE gold_pass_daily_kpis
-- ================================================================

CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_pass_daily_kpis
TARGET_LAG = '30 minutes'
WAREHOUSE = COMPUTE_WH
COMMENT = 'Daily Pass KPIs: trends, adoption, Pass vs non-Pass comparison'
AS
WITH daily_bookings AS (
    -- Get all bookings with Pass flag
    SELECT
        f.booking_date,
        f.booking_id,
        f.customer_key,
        f.total_amount,
        f.booking_category,
        f.quantity,
        f.discount_applied,
        f.pass_ticket_discount,
        f.pass_snack_discount,
        f.pass_dining_voucher,
        f.restaurant_instant_discount,
        f.is_cancelled,
        f.rating,
        
        -- Pass flag
        CASE 
            WHEN f.pass_subscription_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS is_pass_booking,
        
        -- Total Pass discounts
        COALESCE(f.pass_ticket_discount, 0) 
        + COALESCE(f.pass_snack_discount, 0)
        + COALESCE(f.pass_dining_voucher, 0)
        + COALESCE(f.restaurant_instant_discount, 0) AS total_pass_discount
        
    FROM DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f
    WHERE f.booking_date IS NOT NULL
),
date_dimension AS (
    -- Get date attributes
    SELECT
        full_date,
        date_key,
        year,
        quarter,
        month,
        month_name,
        week_of_year,
        day_of_month,
        day_name,
        is_weekend,
        is_holiday
    FROM DISTRICT_ANALYTICS_DB.silver_layer.dim_date
    WHERE full_date <= CURRENT_DATE()
)
SELECT
    -- DATE DIMENSIONS
    d.full_date AS booking_date,
    d.date_key,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year ,
    d.day_of_month,
    d.day_name,
    d.is_weekend,
    d.is_holiday,
    
    -- OVERALL DAILY METRICS (All Bookings)
    COUNT(DISTINCT db.booking_id) AS total_bookings,
    COUNT(DISTINCT db.customer_key) AS total_unique_customers,
    COALESCE(SUM(db.total_amount), 0) AS total_revenue,
    ROUND(AVG(db.total_amount), 2) AS avg_booking_value,
    COALESCE(SUM(db.quantity), 0) AS total_tickets_sold,
    
    -- PASS BOOKINGS (With Pass)
    COUNT(DISTINCT CASE 
        WHEN db.is_pass_booking THEN db.booking_id 
    END) AS pass_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN db.is_pass_booking THEN db.customer_key 
    END) AS pass_unique_customers,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.total_amount ELSE 0 
    END) AS pass_revenue,
    
    ROUND(AVG(CASE 
        WHEN db.is_pass_booking THEN db.total_amount 
    END), 2) AS pass_avg_booking_value,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.quantity ELSE 0 
    END) AS pass_tickets_sold,
    
    -- NON-PASS BOOKINGS (Without Pass)
    COUNT(DISTINCT CASE 
        WHEN NOT db.is_pass_booking THEN db.booking_id 
    END) AS non_pass_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN NOT db.is_pass_booking THEN db.customer_key 
    END) AS non_pass_unique_customers,
    
    SUM(CASE 
        WHEN NOT db.is_pass_booking THEN db.total_amount ELSE 0 
    END) AS non_pass_revenue,
    
    ROUND(AVG(CASE 
        WHEN NOT db.is_pass_booking THEN db.total_amount 
    END), 2) AS non_pass_avg_booking_value,
    
    SUM(CASE 
        WHEN NOT db.is_pass_booking THEN db.quantity ELSE 0 
    END) AS non_pass_tickets_sold,
    
    -- PASS PENETRATION & ADOPTION
    ROUND(
        COUNT(DISTINCT CASE WHEN db.is_pass_booking THEN db.booking_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT db.booking_id), 0),
        2
    ) AS pass_penetration_pct,
    
    ROUND(
        SUM(CASE WHEN db.is_pass_booking THEN db.total_amount ELSE 0 END) * 100.0 
        / NULLIF(SUM(db.total_amount), 0),
        2
    ) AS pass_revenue_contribution_pct,
    
    -- PASS DISCOUNTS & BENEFITS
    COALESCE(SUM(db.total_pass_discount), 0) AS total_pass_discounts_given,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.pass_ticket_discount ELSE 0 
    END) AS pass_movie_discounts,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.pass_dining_voucher ELSE 0 
    END) AS pass_dining_vouchers,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.pass_snack_discount ELSE 0 
    END) AS pass_snack_discounts,
    
    SUM(CASE 
        WHEN db.is_pass_booking THEN db.restaurant_instant_discount ELSE 0 
    END) AS pass_restaurant_instant_discounts,
    
    -- Pass discount as % of Pass revenue
    ROUND(
        SUM(db.total_pass_discount) * 100.0 
        / NULLIF(SUM(CASE WHEN db.is_pass_booking THEN db.total_amount ELSE 0 END), 0),2) AS pass_discount_rate_pct,
    
    -- PASS CATEGORY BREAKDOWN
    COUNT(DISTINCT CASE 
        WHEN db.is_pass_booking AND db.booking_category = 'Movies' 
        THEN db.booking_id 
    END) AS pass_movie_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN db.is_pass_booking AND db.booking_category IN ('Dining', 'Food Festival') 
        THEN db.booking_id 
    END) AS pass_dining_bookings,
    
    COUNT(DISTINCT CASE 
        WHEN db.is_pass_booking AND db.booking_category = 'Movies - Snacks' 
        THEN db.booking_id 
    END) AS pass_snack_bookings,
    
    -- Average booking value difference (Pass users spend more/less?)
    ROUND(
        AVG(CASE WHEN db.is_pass_booking THEN db.total_amount END) 
        - AVG(CASE WHEN NOT db.is_pass_booking THEN db.total_amount END),
        2
    ) AS pass_avg_value_difference,
    
    -- Tickets per booking comparison
    ROUND(
        AVG(CASE WHEN db.is_pass_booking THEN db.quantity END) 
        - AVG(CASE WHEN NOT db.is_pass_booking THEN db.quantity END),
        2
    ) AS pass_tickets_per_booking_difference,
    
    -- ENGAGEMENT METRICS
    ROUND(
        COUNT(DISTINCT CASE 
            WHEN db.is_pass_booking AND db.is_cancelled THEN db.booking_id 
        END) * 100.0 
        / NULLIF(COUNT(DISTINCT CASE WHEN db.is_pass_booking THEN db.booking_id END), 0),
        2) AS pass_cancellation_rate_pct,
    
    -- Non-Pass cancellation rate
    ROUND(
        COUNT(DISTINCT CASE 
            WHEN NOT db.is_pass_booking AND db.is_cancelled THEN db.booking_id 
        END) * 100.0 
        / NULLIF(COUNT(DISTINCT CASE WHEN NOT db.is_pass_booking THEN db.booking_id END), 0),
        2) AS non_pass_cancellation_rate_pct,
    
    -- Pass rating
    ROUND(AVG(CASE WHEN db.is_pass_booking THEN db.rating END), 2) AS pass_avg_rating,
    
    -- Non-Pass rating
    ROUND(AVG(CASE WHEN NOT db.is_pass_booking THEN db.rating END), 2) AS non_pass_avg_rating,
    
    -- MOVING AVERAGES (7-day and 30-day trends)
    ROUND(
    AVG(COALESCE(SUM(CASE WHEN db.is_pass_booking THEN db.total_amount ELSE 0 END), 0))
    OVER (ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
2) AS pass_revenue_7day_ma,
    
    ROUND(
        AVG(COUNT(DISTINCT CASE WHEN db.is_pass_booking THEN db.booking_id END)) 
        OVER (ORDER BY d.full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        2) AS pass_bookings_30day_ma ,
    ROUND(
        AVG(SUM(CASE WHEN db.is_pass_booking THEN db.total_amount ELSE 0 END)) 
        OVER (ORDER BY d.full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        2) AS pass_revenue_30day_ma ,
    ROUND(
    SUM(COUNT(DISTINCT CASE WHEN db.is_pass_booking THEN db.booking_id END))
        OVER (ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)* 100.0 /
    NULLIF(SUM(COUNT(DISTINCT db.booking_id))
            OVER (ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0),2) AS pass_penetration_7day_ma,
    -- Add after the existing PASS category breakdown section:

-- NON-PASS CATEGORY BREAKDOWN
COUNT(DISTINCT CASE 
    WHEN NOT db.is_pass_booking AND db.booking_category = 'Movies' 
    THEN db.booking_id 
END) AS non_pass_movie_bookings,

COUNT(DISTINCT CASE 
    WHEN NOT db.is_pass_booking AND db.booking_category IN ('Dining', 'Food Festival') 
    THEN db.booking_id 
END) AS non_pass_dining_bookings,

COUNT(DISTINCT CASE 
    WHEN NOT db.is_pass_booking AND db.booking_category = 'Movies - Snacks' 
    THEN db.booking_id 
END) AS non_pass_snack_bookings,
    
    -- METADATA
    CURRENT_TIMESTAMP() AS last_updated

FROM date_dimension d
LEFT JOIN daily_bookings db
    ON d.full_date = db.booking_date

GROUP BY
    d.full_date,
    d.date_key,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_month,
    d.day_name,
    d.is_weekend,
    d.is_holiday;

alter dynamic table gold_pass_daily_kpis refresh ;
select * from gold_pass_daily_kpis ;



