-- ================================================================
-- CREATE DIM_PASS_PLAN (SCD TYPE 2)
-- ================================================================
-- Execute Date: 2026-02-25
-- Purpose: Dimension table for District Pass plan definitions with full history
-- Type: SCD Type 2 (maintains historical versions)
-- ================================================================

-- Ensure we're in the correct database and schema
USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;

-- Create the dimension table
CREATE OR REPLACE TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN (
    PASS_PLAN_KEY NUMBER(38,0) IDENTITY(1,1),
    PASS_PLAN_ID VARCHAR(50) NOT NULL COMMENT 'Business key (same across versions)',
    
    -- Plan details
    PLAN_NAME VARCHAR(100) NOT NULL,
    PLAN_DESCRIPTION VARCHAR(500),
    PLAN_PRICE NUMBER(10,2) NOT NULL,
    VALIDITY_DAYS NUMBER(3,0) NOT NULL,
    
    -- Movie benefits
    MOVIE_BENEFIT_LIMIT NUMBER(2,0) COMMENT 'Max free tickets allowed (e.g., 3)',
    MOVIE_MIN_TICKETS NUMBER(2,0) DEFAULT 2 COMMENT 'Min tickets required to use benefit (e.g., 2)',
    
    -- Dining benefits
    DINING_BENEFIT_LIMIT NUMBER(2,0) COMMENT 'Max dining vouchers allowed (e.g., 2)',
    DINING_VOUCHER_VALUE NUMBER(10,2) COMMENT 'Value per voucher (e.g., ₹250)',
    DINING_MIN_BILL_AMOUNT NUMBER(10,2) COMMENT 'Min bill for voucher + instant discount (e.g., ₹1500)',
    DINING_INSTANT_DISCOUNT_PCT NUMBER(5,2) COMMENT 'Restaurant instant discount % (e.g., 10)',
    DINING_MAX_INSTANT_DISCOUNT NUMBER(10,2) COMMENT 'Max instant discount amount (e.g., ₹1500)',
    
    -- Snack benefits
    SNACK_DISCOUNT_PCT NUMBER(5,2) COMMENT 'Snack discount % (e.g., 20)',
    SNACK_MAX_DISCOUNT NUMBER(10,2) COMMENT 'Max snack discount per booking (e.g., ₹200)',
    SNACK_MIN_ORDER NUMBER(10,2) COMMENT 'Min snack order for discount (e.g., ₹750)',
    SNACK_IS_UNLIMITED BOOLEAN DEFAULT TRUE COMMENT 'Whether snack benefit has usage limit',
    
    -- SCD Type 2 fields
    EFFECTIVE_FROM_DATE DATE NOT NULL COMMENT 'When this version became effective',
    EFFECTIVE_TO_DATE DATE COMMENT 'When this version expired (NULL = current)',
    IS_CURRENT BOOLEAN DEFAULT TRUE COMMENT 'TRUE for current version, FALSE for historical',
    VERSION_NUMBER NUMBER(5,0) DEFAULT 1 COMMENT 'Version number (1, 2, 3...)',
    
    -- Status
    IS_ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'Whether plan is available for new purchases',
    
    -- Audit columns
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dim_pass_plan PRIMARY KEY (PASS_PLAN_KEY)
);

-- Add table comment
COMMENT ON TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN 
    IS 'SCD Type 2 dimension table storing District Pass plan definitions with full version history';

-- Insert the current District Pass plan (₹199 for 90 days) - Version 1
INSERT INTO DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN (
    PASS_PLAN_ID,
    PLAN_NAME,
    PLAN_DESCRIPTION,
    PLAN_PRICE,
    VALIDITY_DAYS,
    
    MOVIE_BENEFIT_LIMIT,
    MOVIE_MIN_TICKETS,
    
    DINING_BENEFIT_LIMIT,
    DINING_VOUCHER_VALUE,
    DINING_MIN_BILL_AMOUNT,
    DINING_INSTANT_DISCOUNT_PCT,
    DINING_MAX_INSTANT_DISCOUNT,
    
    SNACK_DISCOUNT_PCT,
    SNACK_MAX_DISCOUNT,
    SNACK_MIN_ORDER,
    SNACK_IS_UNLIMITED,
    
    EFFECTIVE_FROM_DATE,
    EFFECTIVE_TO_DATE,
    IS_CURRENT,
    VERSION_NUMBER,
    IS_ACTIVE
) VALUES (
    'DISTRICT_PASS_Q1_2026',
    'District Pass - Quarterly',
    'Get 3 free movie tickets, 2 dining vouchers worth ₹250 each, and 20% off on movie snacks',
    199.00,
    90,
    
    -- Movie benefits: Max 3 free tickets, min 2 tickets per booking
    3,
    2,
    
    -- Dining benefits: 2 vouchers x ₹250, min ₹1500 bill, 10% instant discount (max ₹1500)
    2,
    250.00,
    1500.00,
    10.00,
    1500.00,
    
    -- Snack benefits: 20% off (max ₹200), min order ₹750, unlimited usage
    20.00,
    200.00,
    750.00,
    TRUE,
    
    -- SCD Type 2 fields
    '2026-01-01',  -- Effective from
    NULL,          -- Effective to (NULL = current)
    TRUE,          -- Is current
    1,             -- Version 1
    TRUE           -- Is active
);

-- Verify the insert
SELECT 
    PASS_PLAN_KEY,
    PASS_PLAN_ID,
    PLAN_NAME,
    PLAN_PRICE,
    VALIDITY_DAYS,
    MOVIE_BENEFIT_LIMIT,
    DINING_BENEFIT_LIMIT,
    SNACK_DISCOUNT_PCT,
    VERSION_NUMBER,
    IS_CURRENT,
    EFFECTIVE_FROM_DATE,
    EFFECTIVE_TO_DATE
FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN
ORDER BY PASS_PLAN_ID, VERSION_NUMBER;


UPDATE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_PLAN
SET PLAN_PRICE = 199.00
WHERE PASS_PLAN_ID = 'DISTRICT_PASS_MONTHLY';
