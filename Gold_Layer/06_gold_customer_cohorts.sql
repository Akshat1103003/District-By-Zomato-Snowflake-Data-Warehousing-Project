-- =====================================================================================
    -- TABLE 6: gold_customer_cohorts
    -- Purpose: Customer cohort analysis and retention metrics
    -- =====================================================================================
CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.gold_layer.gold_customer_cohorts
    TARGET_LAG = '25 minute' 
    WAREHOUSE = COMPUTE_WH 
    COMMENT = 'Customer cohort analysis - acquisition retention and lifetime value by cohort'
AS 
WITH customer_first_booking AS (     
    SELECT
        c.customer_key,
        c.customer_id,
        DATE_TRUNC('month', MIN(f.booking_date)) AS cohort_month,
        MIN(f.booking_date) AS first_booking_date
    FROM
        DISTRICT_ANALYTICS_DB.silver_layer.dim_customer c
        INNER JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
            ON c.customer_key = f.customer_key
    WHERE
        c.is_current = TRUE
    GROUP BY
        c.customer_key,
        c.customer_id
),
customer_monthly_activity AS (
    SELECT
        cfb.cohort_month,
        DATE_TRUNC('month', f.booking_date) AS activity_month,
        DATEDIFF(
            'month',
            cfb.cohort_month,
            DATE_TRUNC('month', f.booking_date)
        ) AS months_since_first,
        COUNT(DISTINCT f.customer_key) AS active_customers,
        COUNT(DISTINCT f.booking_id) AS total_bookings,
        SUM(f.total_amount) AS total_revenue
    FROM
        customer_first_booking cfb
        INNER JOIN DISTRICT_ANALYTICS_DB.silver_layer.fact_bookings f 
            ON cfb.customer_key = f.customer_key
    GROUP BY
        cfb.cohort_month,
        DATE_TRUNC('month', f.booking_date),
        DATEDIFF(
            'month',
            cfb.cohort_month,
            DATE_TRUNC('month', f.booking_date)
        )
)
SELECT
    -- Cohort identification
    cohort_month,
    YEAR(cohort_month) AS cohort_year,
    MONTHNAME(cohort_month) AS cohort_month_name,
    
    -- Activity period
    activity_month,
    months_since_first,
    
    -- Cohort size (at acquisition)
    FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY months_since_first
    ) AS cohort_size,
    -- Current period metrics
    active_customers,
    total_bookings,
    total_revenue,
    
    -- Retention rate
    ROUND(
        active_customers * 100.0 / NULLIF(
            FIRST_VALUE(active_customers) OVER (
                PARTITION BY cohort_month
                ORDER BY months_since_first ),0 ),2 ) AS retention_rate_pct,
    
    -- Engagement metrics
    ROUND(
        total_bookings * 1.0 / NULLIF(active_customers, 0),
        2) AS bookings_per_customer,
    
    ROUND(total_revenue / NULLIF(active_customers, 0), 2) AS revenue_per_customer,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS last_updated
FROM customer_monthly_activity;

select * from gold_customer_cohorts ;
describe table gold_customer_cohorts ;
ALTER DYNAMIC TABLE  gold_customer_metrics REFRESH ;

