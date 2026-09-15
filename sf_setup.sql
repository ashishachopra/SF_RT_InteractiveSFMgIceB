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
-- Generated with correlated fields for realism:
--   - 100M distinct customers; CUSTOMER_EMAIL is derived from CUSTOMER_ID
--   - STORE_ID and STORE_STATE_CD share RANDOM seed 7, so store<->state correlate
--   - ~7 years of history (rolling from today)
--   - CLUSTER BY (CUSTOMER_ID) + ORDER BY on load: essential for point-lookup
--     partition pruning. Without it, every query scans all partitions.
USE WAREHOUSE LOAD_WH;

CREATE OR REPLACE TABLE TXN_HISTORY CLUSTER BY (CUSTOMER_ID) AS
    SELECT
        SEQ8()+1 TXN_ID,
        UNIFORM(1, 100000000, RANDOM(1))::NUMBER(10,0) CUSTOMER_ID,
        'user'||CUSTOMER_ID||'@example.com' AS CUSTOMER_EMAIL,
        UPPER(CHR(UNIFORM(65, 90, RANDOM(2)))) || LPAD(UNIFORM(0, 99999999999, RANDOM(6))::NUMBER, 11, '0') AS TXN_NUM,
        -- 7 years of history
        DATEADD('second', UNIFORM(0, 220752000, RANDOM(3)), DATEADD('year', -7, CURRENT_TIMESTAMP()))::TIMESTAMP_LTZ(6) AS TXN_DATE,
        UNIFORM(0, 30, RANDOM(4)) QUANTITY,
        ROUND(UNIFORM(1, 99999, RANDOM(5))::FLOAT / 100, 2) AS UNIT_PRICE,
        'SKU-' || LPAD(UNIFORM(1, 500000, RANDOM())::STRING, 7, '0') || '-' || LPAD(UNIFORM(0,99,RANDOM()), 2, '0')::STRING AS PRODUCT_ID,
        ARRAY_CONSTRUCT(
            'Electronics', 'Grocery', 'Apparel', 'Home & Garden', 'Sports',
            'Automotive', 'Health & Beauty', 'Toys & Games', 'Office Supplies', 'Pet Supplies',
            'Jewelry', 'Books & Media', 'Furniture', 'Kitchen & Dining', 'Baby & Kids',
            'Outdoor & Camping', 'Tools & Hardware', 'Travel & Luggage', 'Music & Instruments', 'Arts & Crafts'
        )[UNIFORM(0, 19, RANDOM(6))]::STRING AS PRODUCT_CATEGORY,
        UNIFORM(0, 700, RANDOM(7)) STORE_ID,
        -- Same seed (RANDOM(7)) as STORE_ID so stores and states are correlated
        ARRAY_CONSTRUCT(
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
            'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
            'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
        )[MOD(UNIFORM(0, 700, RANDOM(7)), 51)]::STRING AS STORE_STATE_CD
    FROM TABLE(GENERATOR(ROWCOUNT => 1000000000))
    ORDER BY CUSTOMER_ID;

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

INSERT INTO TXN_HISTORY_IT SELECT * FROM TXN_HISTORY ORDER BY CUSTOMER_ID;

-- Verify
SELECT COUNT(*) AS row_count FROM TXN_HISTORY_IT;
-- Expected: 1,000,000,000

-- ---------------------------------------------------------------------------
-- 5. External Iceberg Table (IB) — OPTIONAL · REQUIRES AN EXTERNAL CATALOG
-- ---------------------------------------------------------------------------
-- Skip this entire section unless you want the third (Iceberg) table type.
-- It is NOT required for the FDN or IT benchmarks.
--
-- Third table type: a Snowflake-managed Apache Iceberg table living in a
-- catalog-linked database (this example: ICE_DB_CLD). Because the database is
-- catalog-linked, the table inherits its external volume and catalog from the
-- database — there is no per-table EXTERNAL_VOLUME / CATALOG clause.
--
-- PREREQUISITE (environment-specific — you must create this first):
--   A catalog-linked database backed by your Iceberg catalog + external volume.
--   See: https://docs.snowflake.com/en/user-guide/tables-iceberg
--
-- TARGET_FILE_SIZE = '16MB' produces small files well-suited to the selective
-- point-lookup / small-aggregation query pattern.
--
-- The benchmark references this table with the label "IB". If you skip this
-- section, simply don't pass "--tables IB" to benchmark.py (and remove the
-- IB entry from the TABLES dict in benchmark.py).
--
-- Replace <catalog_linked_db> and <namespace> with your own.
--
-- CREATE OR REPLACE ICEBERG TABLE <catalog_linked_db>."<namespace>"."txn_history_IB" (
--     TXN_ID           NUMBER(19,0),
--     CUSTOMER_ID      NUMBER(10,0),
--     CUSTOMER_EMAIL   STRING,
--     TXN_NUM          STRING,
--     TXN_DATE         TIMESTAMP_NTZ(6),
--     QUANTITY         NUMBER(2,0),
--     UNIT_PRICE       FLOAT,
--     PRODUCT_ID       STRING,
--     PRODUCT_CATEGORY STRING,
--     STORE_ID         NUMBER(4,0),
--     STORE_STATE_CD   STRING
-- )
-- TARGET_FILE_SIZE = '16MB';
--
-- -- Load from the standard table, preserving clustering order:
-- INSERT OVERWRITE INTO <catalog_linked_db>."<namespace>"."txn_history_IB"
-- SELECT * FROM SNOW_DB.SNOW_SCHEMA.TXN_HISTORY
-- ORDER BY CUSTOMER_ID;

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
