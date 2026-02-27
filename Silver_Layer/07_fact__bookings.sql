-- ================================================================
-- FACT_BOOKINGS - SILVER LAYER :  
-- ================================================================
-- Implementation: Dynamic Table , Grain: One row per booking
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;


CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BOOKINGS
TARGET_LAG   = '5 minutes'
REFRESH_MODE = AUTO
INITIALIZE   = ON_CREATE
WAREHOUSE    = COMPUTE_WH
COMMENT      = 'Booking fact table. Auto-refreshes from Bronze Layer.'
AS
SELECT
    -- KEYS & IDs
    TRIM(raw.BOOKING_ID) AS BOOKING_ID,
    c.CUSTOMER_KEY,
    v.VENUE_KEY,
    
    CASE
        WHEN TRIM(raw.BOOKING_CATEGORY) IN ('Movies', 'Movies - Snacks')
        THEN COALESCE(TRIM(raw.EVENT_NAME), TRIM(raw.VENUE_ID))
        ELSE NULL
    END AS MOVIE_ID,
    
    dd.DATE_KEY AS BOOKING_DATE_KEY,
    
    -- BOOKING CATEGORY
    CASE
        WHEN TRIM(raw.BOOKING_CATEGORY) = 'Movies - Snacks' THEN 'Movies - Snacks'
        WHEN TRIM(raw.BOOKING_CATEGORY) = 'Movies'          THEN 'Movies'
        WHEN TRIM(raw.BOOKING_CATEGORY) = 'Dining'          THEN 'Dining'
        WHEN TRIM(raw.BOOKING_CATEGORY) = 'Events'          THEN 'Events'
        WHEN TRIM(raw.BOOKING_CATEGORY) = 'Sports'          THEN 'Sports'
        ELSE TRIM(raw.BOOKING_CATEGORY)
    END AS BOOKING_CATEGORY,
    
    raw.BOOKING_CATEGORY AS RAW_BOOKING_CATEGORY,
    
    raw.BOOKING_STATUS,
    
    -- BOOKING DETAILS
    raw.TICKET_TYPE,
    raw.BOOKING_DATE                           AS BOOKING_DATE,
    raw.BOOKING_TIME                           AS BOOKING_TIME,
    raw.QUANTITY,
    
    -- FINANCIAL - BASE AMOUNTS
    raw.TICKET_BASE_AMOUNT,
    raw.SNACK_AMOUNT,
    ROUND(COALESCE(raw.TICKET_BASE_AMOUNT, 0)
        + COALESCE(raw.SNACK_AMOUNT, 0), 2) AS GROSS_AMOUNT,
    
    -- DISCOUNTS & PROMOS
    raw.PROMO_CODE,
    raw.PROMO_DISCOUNT,
    
    -- PASS BENEFITS
    NULLIF(TRIM(raw.PASS_SUBSCRIPTION_ID), '') AS PASS_SUBSCRIPTION_ID,
    raw.PASS_TICKET_DISCOUNT,
    raw.PASS_SNACK_DISCOUNT,
    raw.PASS_DINING_VOUCHER,
    raw.RESTAURANT_INSTANT_DISCOUNT,
    
    ROUND(COALESCE(raw.PASS_TICKET_DISCOUNT, 0)
        + COALESCE(raw.PASS_SNACK_DISCOUNT, 0)
        + COALESCE(raw.PASS_DINING_VOUCHER, 0), 2) AS TOTAL_PASS_SAVINGS,
    
    CASE
        WHEN NULLIF(TRIM(raw.PASS_SUBSCRIPTION_ID), '') IS NOT NULL
        THEN TRUE ELSE FALSE
    END AS HAS_PASS_BENEFIT,
    
    -- CHARGES & FEES
    raw.BOOKING_CHARGE_BASE,
    raw.BOOKING_CHARGE_GST,
    ROUND(COALESCE(raw.BOOKING_CHARGE_BASE, 0)
        + COALESCE(raw.BOOKING_CHARGE_GST, 0), 2) AS BOOKING_FEE_TOTAL,
    raw.SERVICE_CHARGE,
    raw.COVER_CHARGE_SETTLEMENT,
    raw.TAX_AMOUNT,
    raw.DISCOUNT_APPLIED,
    raw.CONVENIENCE_FEE,
    
    -- TOTALS
    raw.TOTAL_AMOUNT,
    raw.TOTAL_SAVINGS,
    
    -- PAYMENT
    raw.PAYMENT_METHOD,
    raw.PAYMENT_STATUS,
    
    -- Legacy: IS_CANCELLED derived from CANCELLATION_DATE
    CASE 
        WHEN raw.CANCELLATION_DATE IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END  AS IS_CANCELLED,
    
    -- REVIEW
    raw."RATING",                                           -- Quoted (reserved keyword)
    
    -- METADATA
    raw.LOADED_AT                                           AS CREATED_AT,
    CURRENT_TIMESTAMP()                                     AS UPDATED_AT,
    raw.SOURCE_FILE

FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE raw

-- Join DIM_CUSTOMER (IS_CURRENT = TRUE for SCD2 correctness)
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_CUSTOMER c
    ON TRIM(raw.USER_ID) = c.CUSTOMER_ID
    AND c.IS_CURRENT = TRUE

-- Join DIM_VENUE
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_VENUE v
    ON TRIM(raw.VENUE_ID) = v.VENUE_ID

-- Join DIM_DATE
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_DATE dd
    ON raw.BOOKING_DATE = dd.FULL_DATE

WHERE raw.BOOKING_ID IS NOT NULL
  AND TRIM(raw.BOOKING_ID) != '';

SELECT '✅ FACT_BOOKINGS created as Dynamic Table (71-column Bronze aligned)' AS status;

select * from fact_bookings ;
