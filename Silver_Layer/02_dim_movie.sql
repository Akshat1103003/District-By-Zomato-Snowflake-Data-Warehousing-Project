-- ================================================================
-- DIM MOVIE TABLE CREATION : 
-- ================================================================
-- Implementation: Dynamic Table (SCD Type 1)
-- Why Dynamic: Movies don't need history, just latest attributes
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- REATE DIM_MOVIE
-- ================================================================

CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_MOVIE
TARGET_LAG   = '5 minutes'
REFRESH_MODE = AUTO
INITIALIZE   = ON_CREATE
WAREHOUSE    = COMPUTE_WH
COMMENT      = 'Movie dimension. Uses actual Bronze columns (EVENT_NAME, GENRE, LANGUAGE, etc.). SCD Type 1.'
AS
WITH movie_base AS (
    SELECT
        -- ══════════════════════════════════════════════════════
        -- NATURAL KEY: EVENT_NAME (actual movie title)
        -- ══════════════════════════════════════════════════════
        TRIM(EVENT_NAME)                                    AS movie_title,
        
        -- ══════════════════════════════════════════════════════
        -- MOVIE ATTRIBUTES (from Bronze 71 columns)
        -- ══════════════════════════════════════════════════════
        MAX(TRIM(GENRE))                                    AS genre,
        MAX(TRIM(LANGUAGE))                                 AS language,
        MAX(DURATION_MINUTES)                               AS duration_minutes,
        MAX(TRIM(CERTIFICATION))                            AS certification,
  
        -- VENUE CONTEXT (where movie is playing)
        MAX(TRIM(VENUE_ID))                                 AS primary_venue_id,
        MAX(TRIM(VENUE_NAME))                               AS primary_venue_name,
        MAX(TRIM(VENUE_CITY))                               AS primary_venue_city,
          
        -- TICKET FORMAT (experience type)
        MAX(TRIM(TICKET_TYPE))                              AS ticket_type_sample,
  
        -- BOOKING STATS
        COUNT(DISTINCT BOOKING_ID)                          AS total_bookings,
        MIN(TRY_TO_DATE(BOOKING_DATE))                      AS first_booking_date,
        MAX(TRY_TO_DATE(BOOKING_DATE))                      AS last_booking_date,
        SUM(TOTAL_AMOUNT)                                   AS total_revenue
        
    FROM DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE
    WHERE TRIM(BOOKING_CATEGORY) IN ('Movies', 'Movies - Snacks')
      AND EVENT_NAME IS NOT NULL
      AND TRIM(EVENT_NAME) != ''
    GROUP BY TRIM(EVENT_NAME)
)
SELECT
    -- SURROGATE KEY (deterministic HASH)
    ABS(HASH(mb.movie_title))                               AS MOVIE_KEY,
    
    -- NATURAL KEY
    mb.movie_title                                          AS MOVIE_ID,
    mb.movie_title                                          AS MOVIE_TITLE,
    -- MOVIE ATTRIBUTES (actual from Bronze)
    mb.genre                                                AS GENRE,
    mb.language                                             AS LANGUAGE,
    mb.duration_minutes                                     AS DURATION_MINUTES,
    mb.certification                                        AS RATING,
    
    -- VENUE CONTEXT (where primarily playing)
    mb.primary_venue_id                                     AS PRIMARY_VENUE_ID,
    mb.primary_venue_name                                   AS PRIMARY_VENUE_NAME,
    mb.primary_venue_city                                   AS PRIMARY_VENUE_CITY,
  
    -- EXPERIENCE FORMAT
    mb.ticket_type_sample                                   AS TICKET_TYPE_SAMPLE,
    
    -- BOOKING STATS (SCD1 - always latest)
    mb.total_bookings                                       AS TOTAL_BOOKINGS,
    mb.first_booking_date                                   AS FIRST_BOOKING_DATE,
    mb.last_booking_date                                    AS LAST_BOOKING_DATE,
    ROUND(mb.total_revenue, 2)                              AS TOTAL_REVENUE,
    ROUND(mb.total_revenue / NULLIF(mb.total_bookings, 0), 2) AS AVG_REVENUE_PER_BOOKING,
    -- METADATA
    CURRENT_TIMESTAMP()                                     AS UPDATED_AT

FROM movie_base mb;

SELECT '✅ DIM_MOVIE created as Dynamic Table ' AS status;
select * from silver_layer.dim_movie ;
  
alter dynamic table dim_movie refresh ;
