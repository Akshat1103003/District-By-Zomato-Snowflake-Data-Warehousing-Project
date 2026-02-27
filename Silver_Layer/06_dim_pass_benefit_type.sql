-- ================================================================
-- CREATE DIM_PASS_BENEFIT_TYPE (SILVER LAYER)
-- ================================================================
-- Purpose: Dimension table for Pass benefit type definitions
-- Type: SCD Type 1 (simple lookup table)
-- ================================================================

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA SILVER_LAYER;

-- Create the dimension table
CREATE OR REPLACE TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE (
    BENEFIT_TYPE_KEY NUMBER(38,0) IDENTITY(1,1),
    BENEFIT_TYPE_ID VARCHAR(50) UNIQUE NOT NULL COMMENT 'Business key for benefit type',
    
    -- Benefit classification
    BENEFIT_CATEGORY VARCHAR(50) NOT NULL COMMENT 'Movie, Dining, or Snack',
    BENEFIT_NAME VARCHAR(100) NOT NULL,
    BENEFIT_DESCRIPTION VARCHAR(500),
    
    -- Discount mechanics
    DISCOUNT_TYPE VARCHAR(20) NOT NULL COMMENT 'FREE_TICKET, FLAT_AMOUNT, or PERCENTAGE',
    DISCOUNT_VALUE NUMBER(10,2) COMMENT 'Value: 100 for free ticket, 250 for voucher, 20 for 20%',
    
    -- Usage limits
    USAGE_LIMIT NUMBER(10,0) COMMENT 'Max redemptions allowed (NULL = unlimited)',
    MIN_TRANSACTION_VALUE NUMBER(10,2) COMMENT 'Minimum transaction amount to qualify',
    MAX_DISCOUNT_VALUE NUMBER(10,2) COMMENT 'Maximum discount amount per redemption',
    
    -- Status
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT pk_dim_pass_benefit_type PRIMARY KEY (BENEFIT_TYPE_KEY)
);

-- Add table comment
COMMENT ON TABLE DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE 
    IS 'Dimension table storing District Pass benefit type definitions';

-- Insert the 3 benefit types
INSERT INTO DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE (
    BENEFIT_TYPE_ID,
    BENEFIT_CATEGORY,
    BENEFIT_NAME,
    BENEFIT_DESCRIPTION,
    DISCOUNT_TYPE,
    DISCOUNT_VALUE,
    USAGE_LIMIT,
    MIN_TRANSACTION_VALUE,
    MAX_DISCOUNT_VALUE,
    IS_ACTIVE
) VALUES 
-- Movie Benefit: Buy 1 Get 1 Free (Max 3)
(
    'MOVIE_FREE_TICKET',
    'Movie',
    'Buy 1 Get 1 Free Movie Ticket',
    'Get 1 free ticket with every movie booking (min 2 tickets). Lowest-priced ticket is free.',
    'FREE_TICKET',
    100.00,  -- Represents 100% discount on one ticket
    3,       -- Max 3 free tickets per Pass
    NULL,    -- No minimum transaction (but requires 2+ tickets)
    NULL,    -- No cap on ticket price
    TRUE
),

-- Dining Benefit: ₹250 Voucher (Max 2)
(
    'DINING_VOUCHER_250',
    'Dining',
    'Dining Voucher ₹250',
    'Get ₹250 off on dining bills of ₹1500 or above. Can be combined with 10% restaurant instant discount.',
    'FLAT_AMOUNT',
    250.00,  -- ₹250 flat discount
    2,       -- Max 2 vouchers per Pass
    1500.00, -- Min bill ₹1500
    250.00,  -- Max discount ₹250
    TRUE
),

-- Snack Benefit: 20% Off (Unlimited)
(
    'SNACK_DISCOUNT_20PCT',
    'Snack',
    '20% Off on Movie Snacks',
    'Get 20% discount on food & beverages purchased with movie tickets (min order ₹750, max discount ₹200).',
    'PERCENTAGE',
    20.00,   -- 20% discount
    NULL,    -- Unlimited usage
    750.00,  -- Min order ₹750
    200.00,  -- Max discount ₹200
    TRUE
);

-- Verify the inserts
SELECT 
    BENEFIT_TYPE_KEY,
    BENEFIT_TYPE_ID,
    BENEFIT_CATEGORY,
    BENEFIT_NAME,
    DISCOUNT_TYPE,
    DISCOUNT_VALUE,
    USAGE_LIMIT,
    MIN_TRANSACTION_VALUE,
    MAX_DISCOUNT_VALUE,
    IS_ACTIVE
FROM DISTRICT_ANALYTICS_DB.SILVER_LAYER.DIM_PASS_BENEFIT_TYPE
ORDER BY BENEFIT_TYPE_KEY;

select * from dim_pass_benefit_type ;

update  dim_pass_benefit_type set usage_limit =999 where benefit_type_key=3 ;
