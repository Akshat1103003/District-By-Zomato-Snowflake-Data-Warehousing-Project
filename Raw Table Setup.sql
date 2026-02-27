-- ================================================================
-- DISTRICT ANALYTICS - COMPLETE BRONZE LAYER SETUP
-- ================================================================
-- S3 Bucket: district-analytics-raw
-- Region: ap-south-1
-- Folder: raw_folder/
-- Auto-ingest: SQS Queue (Snowflake-managed)
-- Total Columns: 74 (71 original + 3 new)
-- ================================================================

USE ROLE ACCOUNTADMIN;  -- Need ACCOUNTADMIN for storage integration

-- ================================================================
-- STEP 1: CREATE DATABASE AND SCHEMA
-- ================================================================

CREATE DATABASE IF NOT EXISTS DISTRICT_ANALYTICS_DB
COMMENT = 'District by Zomato Analytics - Entertainment booking data warehouse';

CREATE SCHEMA IF NOT EXISTS DISTRICT_ANALYTICS_DB.BRONZE_LAYER
COMMENT = 'Bronze layer - Raw data from S3';

USE DATABASE DISTRICT_ANALYTICS_DB;
USE SCHEMA BRONZE_LAYER;

SELECT '✅ Database and schema created' AS status;

-- ================================================================
-- STEP 2: CREATE STORAGE INTEGRATION
-- ================================================================

CREATE OR REPLACE STORAGE INTEGRATION DISTRICT_S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-role'  -- ⚠️ REPLACE THIS
STORAGE_ALLOWED_LOCATIONS = ('s3://district-analytics-raw/raw_folder/')
COMMENT = 'Storage integration for District Analytics S3 bucket';

SELECT '✅ Storage integration created' AS status;

-- ================================================================
-- IMPORTANT: GET SNOWFLAKE IAM USER ARN
-- ================================================================

DESC STORAGE INTEGRATION DISTRICT_S3_INTEGRATION;

/*
CRITICAL: Copy the following values from the output above:

1. STORAGE_AWS_IAM_USER_ARN (looks like):
   arn:aws:iam::123456789012:user/abc12345-s

2. STORAGE_AWS_EXTERNAL_ID (looks like):
   ABC12345_SFCRole=1_abcdefghijklmnop

YOU NEED THESE FOR AWS IAM ROLE TRUST POLICY!

══════════════════════════════════════════════════════════════════
AWS IAM ROLE SETUP (Do this in AWS Console)
══════════════════════════════════════════════════════════════════

STEP 1: Create IAM Role
────────────────────────
1. AWS Console → IAM → Roles → Create role
2. Trusted entity type: AWS account
3. Account ID: 123456789012 (from STORAGE_AWS_IAM_USER_ARN above)
4. Require external ID: YES → Paste STORAGE_AWS_EXTERNAL_ID
5. Role name: snowflake-role
6. Create role

STEP 2: Attach S3 Permission Policy
─────────────────────────────────────
Go to the role → Add permissions → Create inline policy → JSON:

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::district-analytics-raw",
        "arn:aws:s3:::district-analytics-raw/raw_folder/*"
      ]
    }
  ]
}

Policy name: snowflake-s3-access
Save

STEP 3: Copy Role ARN
──────────────────────
From the role summary page, copy the ARN:
arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-role

STEP 4: Update Storage Integration
────────────────────────────────────
Replace YOUR_AWS_ACCOUNT_ID in the CREATE STORAGE INTEGRATION above
Then re-run the CREATE STORAGE INTEGRATION command

══════════════════════════════════════════════════════════════════
*/

-- ================================================================
-- STEP 3: CREATE FILE FORMAT
-- ================================================================

CREATE OR REPLACE FILE FORMAT DISTRICT_ANALYTICS_DB.BRONZE_LAYER.CSV_FILE_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
NULL_IF = ('NULL', 'null', '', 'None')
EMPTY_FIELD_AS_NULL = TRUE
ENCODING = 'UTF8'
COMMENT = 'CSV file format for District booking data - 74 columns';

SELECT '✅ File format created' AS status;

-- ================================================================
-- STEP 4: CREATE EXTERNAL STAGE
-- ================================================================

CREATE OR REPLACE STAGE DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_STAGE
STORAGE_INTEGRATION = DISTRICT_S3_INTEGRATION
URL = 's3://district-analytics-raw/raw_folder/'
FILE_FORMAT = CSV_FILE_FORMAT
COMMENT = 'External stage pointing to S3 bucket (ap-south-1)';

SELECT '✅ External stage created' AS status;

-- Test stage access
LIST @DISTRICT_STAGE;

/*
If LIST command fails with "Access Denied":
- Verify IAM role is configured correctly
- Check S3 bucket permissions
- Ensure storage integration has correct role ARN
*/

-- ================================================================
-- STEP 5: CREATE BRONZE TABLE (74 COLUMNS)
-- ================================================================

CREATE OR REPLACE TABLE DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE (
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 1-10: CORE BOOKING INFORMATION
    -- ══════════════════════════════════════════════════════════
    BOOKING_ID                      VARCHAR(100)    NOT NULL,
    USER_ID                         VARCHAR(50)     NOT NULL,
    CUSTOMER_NAME                   VARCHAR(100),
    CUSTOMER_CITY                   VARCHAR(50),
    VENUE_ID                        VARCHAR(50)     NOT NULL,
    BOOKING_DATE                    DATE            NOT NULL,
    BOOKING_TIME                    TIME(9),
    BOOKING_CATEGORY                VARCHAR(50),
    TICKET_TYPE                     VARCHAR(50),
    QUANTITY                        NUMBER(5,0),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 11-20: FINANCIAL - BASE AMOUNTS
    -- ══════════════════════════════════════════════════════════
    TICKET_BASE_AMOUNT              NUMBER(10,2),
    SNACK_AMOUNT                    NUMBER(10,2),
    PROMO_CODE                      VARCHAR(50),
    PROMO_DISCOUNT                  NUMBER(10,2),
    PASS_SUBSCRIPTION_ID            VARCHAR(50),
    PASS_TICKET_DISCOUNT            NUMBER(10,2),
    PASS_SNACK_DISCOUNT             NUMBER(10,2),
    PASS_DINING_VOUCHER             NUMBER(10,2),
    RESTAURANT_INSTANT_DISCOUNT     NUMBER(10,2),
    BOOKING_CHARGE_BASE             NUMBER(10,2),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 21-30: FINANCIAL - CHARGES & TOTALS
    -- ══════════════════════════════════════════════════════════
    BOOKING_CHARGE_GST              NUMBER(10,2),
    SERVICE_CHARGE                  NUMBER(10,2),
    COVER_CHARGE_SETTLEMENT         NUMBER(10,2),
    TAX_AMOUNT                      NUMBER(10,2),
    DISCOUNT_APPLIED                NUMBER(10,2),
    CONVENIENCE_FEE                 NUMBER(10,2),
    TOTAL_AMOUNT                    NUMBER(10,2),
    TOTAL_SAVINGS                   NUMBER(10,2),
    PAYMENT_METHOD                  VARCHAR(50),
    PAYMENT_STATUS                  VARCHAR(20),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 31-33: RATING & AUDIT
    -- ══════════════════════════════════════════════════════════
    "RATING"                        NUMBER(3,1),    -- Quoted (reserved keyword)
    LOADED_AT                       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE                     VARCHAR(255),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 34-40: USER DEMOGRAPHICS
    -- ══════════════════════════════════════════════════════════
    USER_EMAIL                      VARCHAR(200),
    USER_PHONE                      VARCHAR(20),
    USER_STATE                      VARCHAR(50),
    USER_COUNTRY                    VARCHAR(50),
    USER_SIGNUP_DATE                DATE,
    USER_SEGMENT                    VARCHAR(20),
    USER_LIFETIME_VALUE             NUMBER(10,2),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 41-44: BOOKING STATUS
    -- ══════════════════════════════════════════════════════════
    PREFERRED_CATEGORY              VARCHAR(50),
    BOOKING_STATUS                  VARCHAR(50),
    CANCELLATION_DATE               DATE,
    CANCELLATION_REASON             VARCHAR(255),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 45-52: EVENT/MOVIE DETAILS
    -- ══════════════════════════════════════════════════════════
    EVENT_NAME                      VARCHAR(200),
    GENRE                           VARCHAR(50),
    LANGUAGE                        VARCHAR(50),
    DURATION_MINUTES                NUMBER(5,0),
    EVENT_TYPE                      VARCHAR(50),
    CERTIFICATION                   VARCHAR(10),
    EVENT_DATE                      DATE,
    EVENT_TIME                      TIME(9),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 53-60: VENUE DETAILS
    -- ══════════════════════════════════════════════════════════
    VENUE_NAME                      VARCHAR(200),
    VENUE_TYPE                      VARCHAR(50),
    VENUE_CITY                      VARCHAR(50),
    VENUE_STATE                     VARCHAR(50),
    VENUE_CAPACITY                  NUMBER(10,0),
    SCREEN_NUMBER                   VARCHAR(50),
    SEATING_SECTION                 VARCHAR(50),
    LATITUDE                        NUMBER(10,6),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 61-64: LOCATION & PRICING
    -- ══════════════════════════════════════════════════════════
    LONGITUDE                       NUMBER(10,6),
    BASE_PRICE                      NUMBER(10,2),
    DYNAMIC_PRICING_FLAG            VARCHAR(10),
    PROCESSING_FEE                  NUMBER(10,2),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 65-71: PAYMENT & REVIEW
    -- ══════════════════════════════════════════════════════════
    PAYMENT_ID                      VARCHAR(100),
    PAYMENT_PROVIDER                VARCHAR(50),
    TRANSACTION_ID                  VARCHAR(100),
    REVIEW_ID                       VARCHAR(100),
    REVIEW_TEXT                     VARCHAR(1000),
    REVIEW_DATE                     DATE,
    REVIEW_STATUS                   VARCHAR(50),
    
    -- ══════════════════════════════════════════════════════════
    -- COLUMNS 72-74: NEW COLUMNS (Enhanced Schema)
    -- ══════════════════════════════════════════════════════════
    VENUE_PRICING_TIER              VARCHAR(50)     COMMENT 'Budget, Mid-Range, Premium, Luxury',
    AGE                             VARCHAR(50)     COMMENT 'Customer age (will be NUMBER in Silver)',
    CUSTOMER_GENDER                 VARCHAR(10)     COMMENT 'Male, Female, Other',
    
    -- ══════════════════════════════════════════════════════════
    -- PRIMARY KEY
    -- ══════════════════════════════════════════════════════════
    CONSTRAINT PK_BOOKING_ID PRIMARY KEY (BOOKING_ID)
)
COMMENT = 'Bronze layer raw table with all 74 columns for District by Zomato bookings';

SELECT '✅ Bronze table created with 74 columns' AS status;

-- Verify column count
SELECT COUNT(*) AS total_columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'BRONZE_LAYER'
  AND TABLE_NAME = 'DISTRICT_RAW_TABLE';

-- Should return 74

-- ================================================================
-- STEP 6: CREATE SNOWPIPE (SQS AUTO-INGEST)
-- ================================================================

CREATE OR REPLACE PIPE DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_BRONZE_SNOWPIPE
COMMENT = 'Auto-ingest pipe for District bookings - 74 columns via SQS'
AS
COPY INTO DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE (
    BOOKING_ID, USER_ID, CUSTOMER_NAME, CUSTOMER_CITY, VENUE_ID,
    BOOKING_DATE, BOOKING_TIME, BOOKING_CATEGORY, TICKET_TYPE, QUANTITY,
    TICKET_BASE_AMOUNT, SNACK_AMOUNT, PROMO_CODE, PROMO_DISCOUNT,
    PASS_SUBSCRIPTION_ID, PASS_TICKET_DISCOUNT, PASS_SNACK_DISCOUNT,
    PASS_DINING_VOUCHER, RESTAURANT_INSTANT_DISCOUNT,
    BOOKING_CHARGE_BASE, BOOKING_CHARGE_GST, SERVICE_CHARGE,
    COVER_CHARGE_SETTLEMENT, TAX_AMOUNT, DISCOUNT_APPLIED,
    CONVENIENCE_FEE, TOTAL_AMOUNT, TOTAL_SAVINGS,
    PAYMENT_METHOD, PAYMENT_STATUS, RATING,
    LOADED_AT, SOURCE_FILE,
    USER_EMAIL, USER_PHONE, USER_STATE, USER_COUNTRY,
    USER_SIGNUP_DATE, USER_SEGMENT, USER_LIFETIME_VALUE,
    PREFERRED_CATEGORY, BOOKING_STATUS, CANCELLATION_DATE,
    CANCELLATION_REASON, EVENT_NAME, GENRE, LANGUAGE,
    DURATION_MINUTES, EVENT_TYPE, CERTIFICATION,
    EVENT_DATE, EVENT_TIME,
    VENUE_NAME, VENUE_TYPE, VENUE_CITY, VENUE_STATE,
    VENUE_CAPACITY, SCREEN_NUMBER, SEATING_SECTION,
    LATITUDE, LONGITUDE, BASE_PRICE,
    DYNAMIC_PRICING_FLAG, PROCESSING_FEE,
    PAYMENT_ID, PAYMENT_PROVIDER, TRANSACTION_ID,
    REVIEW_ID, REVIEW_TEXT, REVIEW_DATE, REVIEW_STATUS,
    VENUE_PRICING_TIER, AGE, CUSTOMER_GENDER
)
FROM (
    SELECT 
        -- Columns 1-10
        $1::VARCHAR(100),                           -- BOOKING_ID
        $2::VARCHAR(50),                            -- USER_ID
        $3::VARCHAR(100),                           -- CUSTOMER_NAME
        $4::VARCHAR(50),                            -- CUSTOMER_CITY
        $5::VARCHAR(50),                            -- VENUE_ID
        TRY_TO_DATE($6, 'YYYY-MM-DD'),             -- BOOKING_DATE
        TRY_TO_TIME($7),                           -- BOOKING_TIME
        $8::VARCHAR(50),                            -- BOOKING_CATEGORY
        $9::VARCHAR(50),                            -- TICKET_TYPE
        NULLIF($10, '')::NUMBER(5,0),              -- QUANTITY
        
        -- Columns 11-20
        NULLIF($11, '')::NUMBER(10,2),             -- TICKET_BASE_AMOUNT
        NULLIF($12, '')::NUMBER(10,2),             -- SNACK_AMOUNT
        NULLIF($13, ''),                           -- PROMO_CODE
        NULLIF($14, '')::NUMBER(10,2),             -- PROMO_DISCOUNT
        NULLIF($15, ''),                           -- PASS_SUBSCRIPTION_ID
        NULLIF($16, '')::NUMBER(10,2),             -- PASS_TICKET_DISCOUNT
        NULLIF($17, '')::NUMBER(10,2),             -- PASS_SNACK_DISCOUNT
        NULLIF($18, '')::NUMBER(10,2),             -- PASS_DINING_VOUCHER
        NULLIF($19, '')::NUMBER(10,2),             -- RESTAURANT_INSTANT_DISCOUNT
        NULLIF($20, '')::NUMBER(10,2),             -- BOOKING_CHARGE_BASE
        
        -- Columns 21-30
        NULLIF($21, '')::NUMBER(10,2),             -- BOOKING_CHARGE_GST
        NULLIF($22, '')::NUMBER(10,2),             -- SERVICE_CHARGE
        NULLIF($23, '')::NUMBER(10,2),             -- COVER_CHARGE_SETTLEMENT
        NULLIF($24, '')::NUMBER(10,2),             -- TAX_AMOUNT
        NULLIF($25, '')::NUMBER(10,2),             -- DISCOUNT_APPLIED
        NULLIF($26, '')::NUMBER(10,2),             -- CONVENIENCE_FEE
        NULLIF($27, '')::NUMBER(10,2),             -- TOTAL_AMOUNT
        NULLIF($28, '')::NUMBER(10,2),             -- TOTAL_SAVINGS
        $29::VARCHAR(50),                          -- PAYMENT_METHOD
        $30::VARCHAR(20),                          -- PAYMENT_STATUS
        
        -- Column 31
        NULLIF($31, '')::NUMBER(3,1),              -- RATING
        
        -- Columns 32-33: AUTO-GENERATED (don't read from CSV)
        CURRENT_TIMESTAMP(),                        -- LOADED_AT
        METADATA$FILENAME,                          -- SOURCE_FILE
        
        -- Columns 34-40
        $34::VARCHAR(200),                         -- USER_EMAIL
        $35::VARCHAR(20),                          -- USER_PHONE
        $36::VARCHAR(50),                          -- USER_STATE
        $37::VARCHAR(50),                          -- USER_COUNTRY
        TRY_TO_DATE($38, 'YYYY-MM-DD'),           -- USER_SIGNUP_DATE
        $39::VARCHAR(20),                          -- USER_SEGMENT
        NULLIF($40, '')::NUMBER(10,2),             -- USER_LIFETIME_VALUE
        
        -- Columns 41-50
        $41::VARCHAR(50),                          -- PREFERRED_CATEGORY
        $42::VARCHAR(50),                          -- BOOKING_STATUS
        TRY_TO_DATE(NULLIF($43, ''), 'YYYY-MM-DD'), -- CANCELLATION_DATE
        NULLIF($44, ''),                           -- CANCELLATION_REASON
        $45::VARCHAR(200),                         -- EVENT_NAME
        NULLIF($46, ''),                           -- GENRE
        NULLIF($47, ''),                           -- LANGUAGE
        NULLIF($48, '')::NUMBER(5,0),              -- DURATION_MINUTES
        $49::VARCHAR(50),                          -- EVENT_TYPE
        NULLIF($50, ''),                           -- CERTIFICATION
        
        -- Columns 51-60
        TRY_TO_DATE($51, 'YYYY-MM-DD'),           -- EVENT_DATE
        TRY_TO_TIME($52),                          -- EVENT_TIME
        $53::VARCHAR(200),                         -- VENUE_NAME
        $54::VARCHAR(50),                          -- VENUE_TYPE
        $55::VARCHAR(50),                          -- VENUE_CITY
        $56::VARCHAR(50),                          -- VENUE_STATE
        NULLIF($57, '')::NUMBER(10,0),             -- VENUE_CAPACITY
        NULLIF($58, ''),                           -- SCREEN_NUMBER
        NULLIF($59, ''),                           -- SEATING_SECTION
        NULLIF($60, '')::NUMBER(10,6),             -- LATITUDE
        
        -- Columns 61-70
        NULLIF($61, '')::NUMBER(10,6),             -- LONGITUDE
        NULLIF($62, '')::NUMBER(10,2),             -- BASE_PRICE
        $63::VARCHAR(10),                          -- DYNAMIC_PRICING_FLAG
        NULLIF($64, '')::NUMBER(10,2),             -- PROCESSING_FEE
        $65::VARCHAR(100),                         -- PAYMENT_ID
        $66::VARCHAR(50),                          -- PAYMENT_PROVIDER
        $67::VARCHAR(100),                         -- TRANSACTION_ID
        NULLIF($68, ''),                           -- REVIEW_ID
        NULLIF($69, ''),                           -- REVIEW_TEXT
        TRY_TO_DATE(NULLIF($70, ''), 'YYYY-MM-DD'), -- REVIEW_DATE
        
        -- Columns 71-74 (NEW COLUMNS)
        NULLIF($71, ''),                           -- REVIEW_STATUS
        NULLIF($72, ''),                           -- VENUE_PRICING_TIER ✅
        NULLIF($73, ''),                           -- AGE ✅
        NULLIF($74, '')                            -- CUSTOMER_GENDER ✅
        
    FROM @DISTRICT_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FILE_FORMAT');

SELECT '✅ Snowpipe created' AS status;

-- ================================================================
-- STEP 7: GET SQS QUEUE ARN FOR S3 EVENT NOTIFICATION
-- ================================================================

SHOW PIPES LIKE 'DISTRICT_BRONZE_SNOWPIPE' IN SCHEMA BRONZE_LAYER;

/*
CRITICAL: Copy the "notification_channel" value from the output above.

It will look like:
arn:aws:sqs:ap-south-1:123456789012:sf-snowpipe-AIDAI3IZFG...

══════════════════════════════════════════════════════════════════
S3 EVENT NOTIFICATION SETUP (AWS Console)
══════════════════════════════════════════════════════════════════

STEP 1: Configure S3 Event Notification
────────────────────────────────────────
1. AWS Console → S3 → district-analytics-raw bucket
2. Go to "Properties" tab
3. Scroll to "Event notifications" → "Create event notification"

Settings:
─────────
Name: snowpipe-district-auto-ingest
Description: Auto-trigger Snowpipe when CSV files uploaded

Prefix: raw_folder/
Suffix: .csv

Event types:
✅ All object create events
   OR specifically:
✅ s3:ObjectCreated:Put
✅ s3:ObjectCreated:Post
✅ s3:ObjectCreated:Copy
✅ s3:ObjectCreated:CompleteMultipartUpload

Destination:
────────────
Select: SQS queue
Enter SQS queue ARN: <PASTE THE notification_channel ARN FROM SHOW PIPES>

Example:
arn:aws:sqs:ap-south-1:123456789012:sf-snowpipe-AIDAI3IZFG...

Click: Save changes

══════════════════════════════════════════════════════════════════

IMPORTANT: S3 must have permission to send to this SQS queue.
The Snowflake-generated SQS queue already has the correct permissions.
*/

-- ================================================================
-- STEP 8: TEST THE SETUP
-- ================================================================

-- Test 1: List files in stage
LIST @DISTRICT_STAGE;

-- Test 2: Manual copy test (validates format)
COPY INTO DISTRICT_RAW_TABLE
FROM @DISTRICT_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FILE_FORMAT')
FILES = ('district_2025_realistic_part01.csv')  -- Adjust to your filename
VALIDATION_MODE = 'RETURN_ERRORS';

/*
If VALIDATION_MODE returns no errors → Format is correct!
If errors → Fix the format/column mapping
*/

-- Test 3: Check pipe status
SELECT SYSTEM$PIPE_STATUS('DISTRICT_BRONZE_SNOWPIPE');

/*
Should show:
{
  "executionState": "RUNNING",
  "pendingFileCount": 0
}
*/

-- ================================================================
-- STEP 9: UPLOAD FILES & VERIFY AUTO-INGEST
-- ================================================================

/*
UPLOAD YOUR CSV FILES TO S3:
────────────────────────────
Path: s3://district-analytics-raw/raw_folder/

Example:
s3://district-analytics-raw/raw_folder/district_2025_realistic_part01.csv
s3://district-analytics-raw/raw_folder/district_2025_realistic_part02.csv
s3://district-analytics-raw/raw_folder/district_2025_realistic_part03.csv
s3://district-analytics-raw/raw_folder/district_2025_realistic_part04.csv

Wait 1-2 minutes for auto-ingest...
*/

-- Check row count (should increase after upload)
SELECT COUNT(*) AS total_rows FROM DISTRICT_RAW_TABLE;

-- Check loaded files
SELECT 
    SOURCE_FILE,
    COUNT(*) AS row_count,
    MIN(LOADED_AT) AS first_loaded,
    MAX(LOADED_AT) AS last_loaded
FROM DISTRICT_RAW_TABLE
GROUP BY SOURCE_FILE
ORDER BY last_loaded DESC;

-- Check copy history
SELECT 
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'DISTRICT_ANALYTICS_DB.BRONZE_LAYER.DISTRICT_RAW_TABLE',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ================================================================
-- STEP 10: MANUAL REFRESH (IF AUTO-INGEST NOT CONFIGURED)
-- ================================================================

-- If you haven't configured S3 event notification yet,
-- you can manually refresh the pipe:

ALTER PIPE DISTRICT_BRONZE_SNOWPIPE REFRESH;

-- Wait 30 seconds, then check again
SELECT COUNT(*) FROM DISTRICT_RAW_TABLE;

-- ================================================================
-- VERIFICATION SUMMARY
-- ================================================================

SELECT '=== SETUP VERIFICATION ===' AS section;

-- 1. Storage Integration
DESC STORAGE INTEGRATION DISTRICT_S3_INTEGRATION;

-- 2. Stage
SHOW STAGES LIKE 'DISTRICT_STAGE';

-- 3. File Format
SHOW FILE FORMATS LIKE 'CSV_FILE_FORMAT';

-- 4. Table columns
SELECT COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'BRONZE_LAYER'
  AND TABLE_NAME = 'DISTRICT_RAW_TABLE';
-- Should return: 74

-- 5. Pipe status
SHOW PIPES LIKE 'DISTRICT_BRONZE_SNOWPIPE';

-- 6. Data loaded
SELECT COUNT(*) AS total_rows FROM DISTRICT_RAW_TABLE;

SELECT '✅ BRONZE LAYER SETUP COMPLETE!' AS message;

/*
================================================================================
SETUP CHECKLIST
================================================================================

✅ Step 1: Database & Schema created
✅ Step 2: Storage Integration created
✅ Step 3: File Format created (CSV with 74 columns)
✅ Step 4: External Stage created (points to S3)
✅ Step 5: Bronze Table created (74 columns)
✅ Step 6: Snowpipe created (SQS auto-ingest)
✅ Step 7: Get SQS ARN for S3 event notification
✅ Step 8: Test setup with validation
✅ Step 9: Upload files & verify auto-ingest
✅ Step 10: Manual refresh option

================================================================================
NEXT STEPS
================================================================================

1. Configure AWS IAM Role (see STEP 2 instructions)
2. Update STORAGE_INTEGRATION with correct IAM role ARN
3. Configure S3 Event Notification (see STEP 7 instructions)
4. Upload CSV files to s3://district-analytics-raw/raw_folder/
5. Wait 1-2 minutes for auto-ingest
6. Verify: SELECT COUNT(*) FROM DISTRICT_RAW_TABLE;

================================================================================
TROUBLESHOOTING
================================================================================

If data not loading:
1. Check: SELECT SYSTEM$PIPE_STATUS('DISTRICT_BRONZE_SNOWPIPE');
2. Check: COPY_HISTORY for errors
3. Verify: S3 event notification configured with correct SQS ARN
4. Test: Manual COPY with VALIDATION_MODE
5. Fallback: ALTER PIPE REFRESH (manual)

================================================================================
*/
