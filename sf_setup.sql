-- =============================================================================
-- IWH Concurrency Benchmark — Setup
-- =============================================================================
-- Creates all objects needed to run the benchmark:
--   1. Database and schema
--   2. Standard table (TXN_HISTORY) with 1B rows, clustered by CUSTOMER_ID
--   3. Interactive table (TXN_HISTORY_IT) — same schema
--   4. Regular warehouse (COMPUTE_XS_WH) — baseline
--   5. Interactive warehouse (TXN_INTERACTIVE_WH) — test subject
--
-- Data generation uses Snowflake's GENERATOR to produce 1B synthetic
-- transaction rows with 100M distinct customers, 51 US states, 20 product
-- categories, and 700 stores over a ~7 year date range.
--
-- Estimated time: ~15-20 minutes on an XL warehouse for the 1B row INSERT.
-- =============================================================================

USE ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 1. Database and Schema
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS SNOW_DB;
CREATE SCHEMA IF NOT EXISTS SNOW_DB.SNOW_SCHEMA;
USE SCHEMA SNOW_DB.SNOW_SCHEMA;

-- ---------------------------------------------------------------------------
-- 2. Warehouses
-- ---------------------------------------------------------------------------

-- Regular warehouse (baseline comparison)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_XS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME  = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Interactive warehouse (benchmark target)
CREATE WAREHOUSE IF NOT EXISTS TXN_INTERACTIVE_WH
    WAREHOUSE_TYPE = 'INTERACTIVE'
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 86400
    AUTO_RESUME  = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Interactive warehouse for txn sales analytics';

-- Temp warehouse for data loading (suspend after setup)
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
    WAREHOUSE_SIZE = 'XLARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME  = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ---------------------------------------------------------------------------
-- 3. Standard Table (FDN) — 1 Billion Rows
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE TXN_HISTORY (
    TXN_ID          NUMBER(20,0),
    CUSTOMER_ID     NUMBER(10,0),
    CUSTOMER_EMAIL  VARCHAR,
    TXN_NUM         VARCHAR,
    TXN_DATE        TIMESTAMP_LTZ,
    QUANTITY         NUMBER(2,0),
    UNIT_PRICE      FLOAT,
    PRODUCT_ID      VARCHAR,
    PRODUCT_CATEGORY VARCHAR,
    STORE_ID        NUMBER(3,0),
    STORE_STATE_CD  VARCHAR
)
CLUSTER BY (CUSTOMER_ID);

-- Generate 1B rows with realistic distributions:
--   - 100M distinct customers (customer_id 1..100,000,000)
--   - Each customer has ~10 transactions on average
--   - 51 US state codes, 20 product categories, 700 stores
--   - Dates spanning ~7 years
--   - Unit prices $1-$500, quantities 1-20
USE WAREHOUSE LOAD_WH;

INSERT INTO TXN_HISTORY
SELECT
    ROW_NUMBER() OVER (ORDER BY SEQ8())                           AS txn_id,
    UNIFORM(1, 100000000, RANDOM())                               AS customer_id,
    'user' || UNIFORM(1, 100000000, RANDOM()) || '@example.com'   AS customer_email,
    'TXN-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ8()), 12, '0') AS txn_num,
    DATEADD('second',
            UNIFORM(0, 220752000, RANDOM()),
            '2019-09-01'::TIMESTAMP_LTZ)                          AS txn_date,
    UNIFORM(1, 20, RANDOM())                                      AS quantity,
    ROUND(UNIFORM(1, 50000, RANDOM()) / 100.0, 2)                AS unit_price,
    'PROD-' || LPAD(UNIFORM(1, 5000, RANDOM()), 5, '0')          AS product_id,
    ARRAY_CONSTRUCT(
        'Electronics','Clothing','Home & Garden','Sports','Toys',
        'Books','Automotive','Health','Food','Beauty',
        'Pet Supplies','Office','Music','Movies','Software',
        'Jewelry','Shoes','Furniture','Appliances','Tools'
    )[UNIFORM(0, 19, RANDOM())]::VARCHAR                          AS product_category,
    UNIFORM(1, 700, RANDOM())                                     AS store_id,
    ARRAY_CONSTRUCT(
        'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
        'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
        'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
        'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
        'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
    )[UNIFORM(0, 50, RANDOM())]::VARCHAR                          AS store_state_cd
FROM TABLE(GENERATOR(ROWCOUNT => 1000000000));

-- Verify row count
SELECT COUNT(*) AS row_count FROM TXN_HISTORY;
-- Expected: 1,000,000,000

-- ---------------------------------------------------------------------------
-- 4. Interactive Table (IT)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE INTERACTIVE TABLE TXN_HISTORY_IT (
    TXN_ID          NUMBER(20,0),
    CUSTOMER_ID     NUMBER(10,0),
    CUSTOMER_EMAIL  VARCHAR,
    TXN_NUM         VARCHAR,
    TXN_DATE        TIMESTAMP_LTZ,
    QUANTITY         NUMBER(2,0),
    UNIT_PRICE      FLOAT,
    PRODUCT_ID      VARCHAR,
    PRODUCT_CATEGORY VARCHAR,
    STORE_ID        NUMBER(3,0),
    STORE_STATE_CD  VARCHAR
)
CLUSTER BY (CUSTOMER_ID);

INSERT INTO TXN_HISTORY_IT SELECT * FROM TXN_HISTORY;

-- Verify
SELECT COUNT(*) AS row_count FROM TXN_HISTORY_IT;
-- Expected: 1,000,000,000

-- ---------------------------------------------------------------------------
-- 5. Attach Interactive Table to Interactive Warehouse
-- ---------------------------------------------------------------------------
ALTER WAREHOUSE TXN_INTERACTIVE_WH RESUME;
ALTER WAREHOUSE TXN_INTERACTIVE_WH ADD TABLES (SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT);

-- ---------------------------------------------------------------------------
-- 6. Cleanup: suspend load warehouse
-- ---------------------------------------------------------------------------
ALTER WAREHOUSE LOAD_WH SUSPEND;

-- ---------------------------------------------------------------------------
-- Verify setup
-- ---------------------------------------------------------------------------
SHOW TABLES LIKE 'TXN_HISTORY%' IN SCHEMA SNOW_DB.SNOW_SCHEMA;
SHOW WAREHOUSES LIKE 'TXN%';
SHOW WAREHOUSES LIKE 'COMPUTE%';
