-- ================================================================
-- DIM_VENUE - SILVER LAYER
-- ================================================================
-- Implementation: Dynamic Table (SCD Type 1)
-- Grain: One row per unique VENUE_ID
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- CREATE DIM_VENUE
-- ================================================================

CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_VENUE
TARGET_LAG   = '5 minutes'
REFRESH_MODE = AUTO
INITIALIZE   = ON_CREATE
WAREHOUSE    = COMPUTE_WH
COMMENT      = 'Venue dimension. One row per VENUE_ID. SCD Type 1 - overwrites on change.'
AS
SELECT
    -- SURROGATE KEY
    ABS(HASH(VENUE_ID))                                     AS VENUE_KEY,
    
    -- NATURAL KEY
    VENUE_ID,
    
    -- VENUE ATTRIBUTES (from Bronze - all aggregated)
    MAX(TRIM(VENUE_NAME))                                   AS VENUE_NAME,
    MAX(TRIM(VENUE_TYPE))                                   AS VENUE_TYPE,
    MAX(TRIM(VENUE_CITY))                                   AS VENUE_CITY,
    MAX(TRIM(VENUE_STATE))                                  AS VENUE_STATE,
    MAX(TRIM(USER_COUNTRY))                                 AS VENUE_COUNTRY,
    
    -- PHYSICAL ATTRIBUTES
    MAX(VENUE_CAPACITY)                                     AS VENUE_CAPACITY,
    
    -- Screen/section aggregation (multiple screens per venue)
    COUNT(DISTINCT SCREEN_NUMBER)                           AS TOTAL_SCREENS,
    LISTAGG(DISTINCT TRIM(SEATING_SECTION), ', ') 
        WITHIN GROUP (ORDER BY TRIM(SEATING_SECTION))       AS SEATING_SECTIONS,
    
    -- Geographic coordinates (average if slight variations)
    ROUND(AVG(LATITUDE), 6)                                 AS LATITUDE,
    ROUND(AVG(LONGITUDE), 6)                                AS LONGITUDE,
    
    -- CATEGORIES (what this venue offers)
    LISTAGG(DISTINCT TRIM(BOOKING_CATEGORY), ', ')
        WITHIN GROUP (ORDER BY TRIM(BOOKING_CATEGORY))      AS VENUE_CATEGORIES,
    max(trim(venue_pricing_tier)) as pricing_tier,
    
    -- RATING & REVIEWS
    ROUND(AVG("RATING"), 1)                                 AS VENUE_RATING,
    COUNT(DISTINCT CASE WHEN "RATING" IS NOT NULL 
                   THEN BOOKING_ID END)                     AS TOTAL_REVIEWS,
    
    -- PASS SUPPORT FLAGS
    MAX(CASE WHEN PASS_DINING_VOUCHER > 0 
        THEN TRUE ELSE FALSE END)                           AS SUPPORTS_PASS_DINING,
    
    MAX(CASE WHEN PASS_TICKET_DISCOUNT > 0 
        THEN TRUE ELSE FALSE END)                           AS SUPPORTS_PASS_MOVIES,
    
    MAX(CASE WHEN PASS_SNACK_DISCOUNT > 0 
        THEN TRUE ELSE FALSE END)                           AS SUPPORTS_PASS_SNACKS,
    
    -- BOOKING STATS
    COUNT(DISTINCT BOOKING_ID)                              AS TOTAL_BOOKINGS,
    COUNT(DISTINCT USER_ID)                                 AS UNIQUE_CUSTOMERS,
    MIN(TRY_TO_DATE(BOOKING_DATE))                          AS FIRST_BOOKING_DATE,
    MAX(TRY_TO_DATE(BOOKING_DATE))                          AS LAST_BOOKING_DATE,
    SUM(TOTAL_AMOUNT)                                       AS TOTAL_REVENUE,
    ROUND(AVG(TOTAL_AMOUNT), 2)                             AS AVG_BOOKING_VALUE,
    
    -- METADATA
    CURRENT_TIMESTAMP()                                     AS UPDATED_AT

FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE
WHERE VENUE_ID IS NOT NULL
  AND TRIM(VENUE_ID) != ''
GROUP BY VENUE_ID;

alter dynamic table dim_venue refresh ;
select * from dim_venue ;
