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
-- 5. External Iceberg Table (IB) — OPTIONAL · REQUIRES AN EXTERNAL CATALOG
-- ---------------------------------------------------------------------------
-- Skip this entire section unless you want the third (Iceberg) table type.
-- It is NOT required for the FDN or IT benchmarks.
-- Third table type for the benchmark: an externally-managed Apache Iceberg
-- table read through a catalog integration (this example uses a Polaris /
-- Open Catalog REST catalog).
--
-- PREREQUISITES (environment-specific — you must create these first):
--   1. An EXTERNAL VOLUME pointing at your object-store location.
--   2. A CATALOG INTEGRATION of your Iceberg catalog.
-- See: https://docs.snowflake.com/en/user-guide/tables-iceberg
--
-- The benchmark references this table with the label "IB". If you skip this
-- section, simply don't pass "--tables IB" to benchmark.py.
--
-- Example (edit CATALOG, EXTERNAL_VOLUME, BASE_LOCATION for your account):
--
-- CREATE OR REPLACE ICEBERG TABLE TXN_HISTORY_IB
--     EXTERNAL_VOLUME    = 'my_external_volume'
--     CATALOG            = 'my_catalog_integration'
--     CATALOG_TABLE_NAME = 'txn_history_IB'
--     CATALOG_NAMESPACE  = 'my_namespace';
--
-- If you manage the Iceberg data outside Snowflake, load it with the same
-- 1B-row shape as TXN_HISTORY so the queries are comparable.

-- ---------------------------------------------------------------------------
-- 6. Masking Policy — OPTIONAL (governance-overhead test)
-- ---------------------------------------------------------------------------
-- Skip this entire section unless you want to measure the cost of column-level
-- governance. It is NOT required for any of the core benchmarks.
--
-- The benchmark's "point_lookup_email" query reads CUSTOMER_EMAIL. Applying a
-- masking policy to that column lets you measure the throughput/latency cost
-- of column-level governance: run point_lookup_email with the policy ATTACHED,
-- then UNSET it and re-run, and compare.
--
-- Create the policy (uncomment to enable):
-- CREATE OR REPLACE MASKING POLICY SNOW_DB.SNOW_SCHEMA.EMAIL_MASK
--     AS (VAL VARCHAR) RETURNS VARCHAR ->
--     CASE
--         WHEN CURRENT_ROLE() IN ('SECURITYADMIN', 'ACCOUNTADMIN') THEN VAL
--         ELSE REGEXP_REPLACE(VAL, '^[^@]+', '***')
--     END;

-- Attach it to CUSTOMER_EMAIL on whichever table(s) you are testing, e.g.:
--   ALTER TABLE TXN_HISTORY_IT
--       MODIFY COLUMN CUSTOMER_EMAIL SET MASKING POLICY SNOW_DB.SNOW_SCHEMA.EMAIL_MASK;
--
-- Run the masking benchmark (as a non-privileged role so masking engages):
--   python benchmark.py --connection my_conn --full \
--       --tables IT --warehouses IWH --queries point_lookup_email --procs 8
--
-- Then detach and re-run the same command to get the un-masked baseline:
--   ALTER TABLE TXN_HISTORY_IT
--       MODIFY COLUMN CUSTOMER_EMAIL UNSET MASKING POLICY;

-- ---------------------------------------------------------------------------
-- 7. Attach Interactive Table to Interactive Warehouse
-- ---------------------------------------------------------------------------
ALTER WAREHOUSE TXN_INTERACTIVE_WH RESUME;
ALTER WAREHOUSE TXN_INTERACTIVE_WH ADD TABLES (SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT);

-- ---------------------------------------------------------------------------
-- 8. Cleanup: suspend load warehouse
-- ---------------------------------------------------------------------------
ALTER WAREHOUSE LOAD_WH SUSPEND;

-- ---------------------------------------------------------------------------
-- Verify setup
-- ---------------------------------------------------------------------------
SHOW TABLES LIKE 'TXN_HISTORY%' IN SCHEMA SNOW_DB.SNOW_SCHEMA;
SHOW WAREHOUSES LIKE 'TXN%';
SHOW WAREHOUSES LIKE 'COMPUTE%';
