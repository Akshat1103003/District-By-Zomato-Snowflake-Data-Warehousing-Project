-- ================================================================
-- 20: FACT_BENEFIT_REDEMPTION - ENHANCED WITH FULL ANALYTICS
-- ================================================================
-- Purpose: Comprehensive benefit redemption tracking with 4 categories
-- Dependencies: FACT_BOOKINGS, FACT_PASS_SUBSCRIPTION, DIM_PASS_BENEFIT_TYPE, DIM_CUSTOMER
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;
USE WAREHOUSE COMPUTE_WH;

-- ================================================================
-- CREATE FACT_BENEFIT_REDEMPTION
-- ================================================================

CREATE OR REPLACE DYNAMIC TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BENEFIT_REDEMPTION
TARGET_LAG   = '10 minutes'
REFRESH_MODE = AUTO
INITIALIZE   = ON_CREATE
WAREHOUSE    = COMPUTE_WH
COMMENT      = 'Enhanced benefit redemption table. Tracks 4 benefit types: Movie, Dining, Snack, Restaurant Instant. Includes lifecycle and re-purchase analytics.'
AS
WITH customer_pass_counts AS (
    -- Calculate how many times each customer bought Pass
    SELECT
        CUSTOMER_KEY,
        COUNT(DISTINCT SUBSCRIPTION_ID)                     AS TOTAL_PASS_PURCHASES
    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_PASS_SUBSCRIPTION
    GROUP BY CUSTOMER_KEY
),
redemptions AS (
    SELECT
        fb.BOOKING_ID,
        fb.CUSTOMER_KEY,
        fb.VENUE_KEY,
        fb.BOOKING_DATE,
        fb.BOOKING_TIME,
        fb.BOOKING_DATE_KEY,
        fb.PASS_SUBSCRIPTION_ID,
        fb.BOOKING_CATEGORY,
        fb.IS_CANCELLED,
        
        -- Financial context
        fb.TOTAL_AMOUNT AS customer_paid_amount,
        fb.PASS_TICKET_DISCOUNT,
        fb.PASS_SNACK_DISCOUNT,
        fb.PASS_DINING_VOUCHER,
        fb.RESTAURANT_INSTANT_DISCOUNT,
        
        -- Booking details
        fb.TICKET_TYPE,
        fb.QUANTITY,
        fb.TICKET_BASE_AMOUNT,
        fb.SNACK_AMOUNT,
        fb.SOURCE_FILE,

        -- BENEFIT CATEGORY (4 types now!)
        CASE
            WHEN fb.PASS_TICKET_DISCOUNT > 0 THEN 'Movie'
            WHEN fb.PASS_DINING_VOUCHER > 0 THEN 'Dining Voucher'
            WHEN fb.RESTAURANT_INSTANT_DISCOUNT > 0 THEN 'Restaurant Instant'
            WHEN fb.PASS_SNACK_DISCOUNT > 0 THEN 'Snack'
        END AS benefit_category,

        -- USAGE SEQUENCE per subscription per benefit type
        ROW_NUMBER() OVER (
            PARTITION BY
                fb.PASS_SUBSCRIPTION_ID,
                CASE
                    WHEN fb.PASS_TICKET_DISCOUNT > 0 THEN 'Movie'
                    WHEN fb.PASS_DINING_VOUCHER > 0 THEN 'Dining Voucher'
                    WHEN fb.RESTAURANT_INSTANT_DISCOUNT > 0 THEN 'Restaurant Instant'
                    WHEN fb.PASS_SNACK_DISCOUNT > 0 THEN 'Snack'
                END
            ORDER BY fb.BOOKING_DATE
        ) AS usage_sequence

    FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_BOOKINGS fb
    WHERE fb.HAS_PASS_BENEFIT = TRUE
       OR fb.RESTAURANT_INSTANT_DISCOUNT > 0  -- Include restaurant instant even if no other benefit
)
SELECT
    -- ══════════════════════════════════════════════════════════
    -- KEYS
    -- ══════════════════════════════════════════════════════════
    ABS(HASH(r.BOOKING_ID || '|' || r.benefit_category))   AS REDEMPTION_KEY,
    'RED_' || UPPER(LEFT(MD5(r.BOOKING_ID || r.benefit_category), 12)) AS REDEMPTION_ID,
    
    -- Foreign keys
    fps.SUBSCRIPTION_KEY,
    r.CUSTOMER_KEY,
    btype.BENEFIT_TYPE_KEY,
    fps.PASS_PLAN_KEY,                                      -- NEW: Link to plan
    r.BOOKING_ID,
    r.BOOKING_DATE_KEY AS REDEMPTION_DATE_KEY,
    r.VENUE_KEY,
    
    -- DATES & TIMING
    r.BOOKING_DATE                                          AS REDEMPTION_DATE,
    r.BOOKING_TIME                                          AS REDEMPTION_TIME,
    
    DATEDIFF('day', fps.START_DATE, r.BOOKING_DATE)         AS DAYS_INTO_PASS,
    
    CASE
        WHEN r.BOOKING_DATE BETWEEN fps.START_DATE AND fps.END_DATE
        THEN TRUE ELSE FALSE
    END                                                     AS REDEMPTION_WITHIN_VALIDITY,
    
    -- BENEFIT DETAILS
    r.benefit_category                                      AS BENEFIT_CATEGORY,
    r.usage_sequence                                        AS USAGE_SEQUENCE,
    
    -- FINANCIAL METRICS 
    -- Original amount (before any discount)
    CASE r.benefit_category
        WHEN 'Movie'               THEN r.TICKET_BASE_AMOUNT
        WHEN 'Dining Voucher'      THEN r.TICKET_BASE_AMOUNT
        WHEN 'Restaurant Instant'  THEN r.TICKET_BASE_AMOUNT
        WHEN 'Snack'               THEN r.SNACK_AMOUNT
    END AS ORIGINAL_AMOUNT,

    -- Discount amount given
    CASE r.benefit_category
        WHEN 'Movie'               THEN r.PASS_TICKET_DISCOUNT
        WHEN 'Dining Voucher'      THEN r.PASS_DINING_VOUCHER
        WHEN 'Restaurant Instant'  THEN r.RESTAURANT_INSTANT_DISCOUNT
        WHEN 'Snack'               THEN r.PASS_SNACK_DISCOUNT
    END AS DISCOUNT_AMOUNT,

    -- Final amount (after discount)
    CASE r.benefit_category
        WHEN 'Movie'  THEN r.TICKET_BASE_AMOUNT - r.PASS_TICKET_DISCOUNT
        WHEN 'Dining Voucher' THEN r.TICKET_BASE_AMOUNT - r.PASS_DINING_VOUCHER
        WHEN 'Restaurant Instant' THEN r.TICKET_BASE_AMOUNT - r.RESTAURANT_INSTANT_DISCOUNT
        WHEN 'Snack'  THEN r.SNACK_AMOUNT - r.PASS_SNACK_DISCOUNT
    END  AS FINAL_AMOUNT,

    -- Benefit theoretical value (from dimension table - no hardcoding!)
    btype.DISCOUNT_VALUE AS BENEFIT_VALUE,
    
    -- NEW: Customer's total spend on this booking
    r.customer_paid_amount AS CUSTOMER_PAID_AMOUNT,
    
    -- NEW: Discount as % of original
    ROUND(
        (CASE r.benefit_category
            WHEN 'Movie'               THEN r.PASS_TICKET_DISCOUNT
            WHEN 'Dining Voucher'      THEN r.PASS_DINING_VOUCHER
            WHEN 'Restaurant Instant'  THEN r.RESTAURANT_INSTANT_DISCOUNT
            WHEN 'Snack'               THEN r.PASS_SNACK_DISCOUNT
        END / NULLIF(
            CASE r.benefit_category
                WHEN 'Movie'               THEN r.TICKET_BASE_AMOUNT
                WHEN 'Dining Voucher'      THEN r.TICKET_BASE_AMOUNT
                WHEN 'Restaurant Instant'  THEN r.TICKET_BASE_AMOUNT
                WHEN 'Snack'               THEN r.SNACK_AMOUNT
            END, 0)) * 100,2) AS DISCOUNT_PCT_OF_ORIGINAL,
    
    -- Savings as % of what customer paid
    ROUND(
        (CASE r.benefit_category
            WHEN 'Movie'               THEN r.PASS_TICKET_DISCOUNT
            WHEN 'Dining Voucher'      THEN r.PASS_DINING_VOUCHER
            WHEN 'Restaurant Instant'  THEN r.RESTAURANT_INSTANT_DISCOUNT
            WHEN 'Snack'               THEN r.PASS_SNACK_DISCOUNT
        END / NULLIF(r.customer_paid_amount, 0)) * 100,
        2
    ) AS CUSTOMER_SAVINGS_PCT,

    -- Forfeited: dining voucher where bill < minimum
    CASE
        WHEN r.benefit_category = 'Dining Voucher'
             AND r.TICKET_BASE_AMOUNT < COALESCE(btype.MIN_TRANSACTION_VALUE, 250)
        THEN COALESCE(btype.MIN_TRANSACTION_VALUE, 250) - r.TICKET_BASE_AMOUNT
        ELSE 0
    END AS FORFEITED_AMOUNT,
    
    -- BOOKING CONTEXT
    r.BOOKING_CATEGORY,                                     -- NEW: Actual booking category
    r.IS_CANCELLED,                                         -- NEW: Cancellation flag
    
    -- MOVIE-SPECIFIC
    CASE WHEN r.benefit_category = 'Movie' 
         THEN r.TICKET_TYPE END                             AS TICKET_CATEGORY,
    CASE WHEN r.benefit_category = 'Movie' 
         THEN r.QUANTITY END                                AS TICKETS_IN_BOOKING,
    CASE WHEN r.benefit_category = 'Movie'
         THEN r.PASS_TICKET_DISCOUNT END                    AS FREE_TICKET_PRICE,

    -- DINING-SPECIFIC
    CASE WHEN r.benefit_category = 'Dining Voucher'
         THEN btype.DISCOUNT_VALUE END                      AS DINING_VOUCHER_VALUE,
    CASE WHEN r.benefit_category = 'Restaurant Instant'
         THEN btype.DISCOUNT_VALUE END                      AS RESTAURANT_DISCOUNT_PCT,
    CASE WHEN r.benefit_category = 'Restaurant Instant'
         THEN r.RESTAURANT_INSTANT_DISCOUNT END             AS RESTAURANT_DISCOUNT_AMT,

    -- SNACK-SPECIFIC
    CASE WHEN r.benefit_category = 'Snack'
         THEN r.SNACK_AMOUNT END                            AS SNACK_SUBTOTAL,
    CASE WHEN r.benefit_category = 'Snack' 
         THEN btype.DISCOUNT_VALUE END                      AS SNACK_DISCOUNT_PCT_APPLIED,
    
    -- CUSTOMER RE-PURCHASE TRACKING 
    cpc.TOTAL_PASS_PURCHASES                                AS CUSTOMER_PASS_PURCHASE_COUNT,
    CASE 
        WHEN cpc.TOTAL_PASS_PURCHASES > 1 THEN TRUE 
        ELSE FALSE 
    END                                                     AS IS_REPEAT_PASS_BUYER,
    
    -- METADATA
    CURRENT_TIMESTAMP()                                     AS UPDATED_AT,
    r.SOURCE_FILE

FROM redemptions r

-- Get SUBSCRIPTION_KEY and plan context from FACT_PASS_SUBSCRIPTION
JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.FACT_PASS_SUBSCRIPTION fps
    ON r.PASS_SUBSCRIPTION_ID = fps.SUBSCRIPTION_ID

-- Get BENEFIT_TYPE_KEY and dimension attributes
LEFT JOIN DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE btype
    ON btype.BENEFIT_TYPE_ID = CASE r.benefit_category
        WHEN 'Movie'               THEN 'MOVIE_FREE_TICKET'
        WHEN 'Dining Voucher'      THEN 'DINING_VOUCHER_250'
        WHEN 'Restaurant Instant'  THEN 'DINING_INSTANT_DISCOUNT_10PCT'
        WHEN 'Snack'               THEN 'SNACK_DISCOUNT_20PCT'
    END

-- Get customer re-purchase counts
LEFT JOIN customer_pass_counts cpc
    ON r.CUSTOMER_KEY = cpc.CUSTOMER_KEY WHERE r.benefit_category IS NOT NULL;

SELECT '✅ FACT_BENEFIT_REDEMPTION (ENHANCED) created - 4 benefit types + analytics!' AS status;

select * from fact_benefit_redemption ;
alter dynamic table fact_benefit_redemption refresh ;
