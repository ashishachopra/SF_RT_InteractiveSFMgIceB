# IWH Concurrency Benchmark

A repeatable benchmark for measuring Snowflake Interactive Warehouse throughput at scale. Tests sub-second query latency under concurrent load from 10 to 1,000 simultaneous queries against a 1-billion-row transaction table.

**Blog**: [Real-Time Analytics on Snowflake: A Repeatable Benchmark for Sub-Second Latency at Scale](https://medium.com/@paul.needleman/real-time-analytics-on-snowflake-a-repeatable-benchmark-for-sub-second-latency-at-scale-49b666f2deb2)

## What It Tests

Two query patterns against 1B rows (100M distinct customers):

| Query | Pattern | Rows Returned | Typical Latency (IWH) |
|---|---|---|---|
| **Point Lookup** | `WHERE customer_id = ?` with aggregation | 1 row | 16-20ms at c=250 |
| **Customer 360** | `WHERE customer_id = ?` with `GROUP BY` | ~10 rows | 19-29ms at c=250 |

Tested on:
- **Interactive Table + Interactive Warehouse** (IWH) — the new architecture
- **Standard Table + Regular Warehouse** (baseline comparison)

## Results Summary

| Configuration | c=250 QPS | c=1000 QPS | p50 at c=250 |
|---|---|---|---|
| Regular WH (XS) | 286 (PL) / 228 (C360) | N/A (queuing) | 617ms / 793ms |
| IWH (XS, 1 cluster) | 1,079 (PL) / 1,094 (C360) | — | 20ms / 29ms |
| IWH (XS, MCW=2) | — | 2,450 (PL) / 2,149 (C360) | — |

## Prerequisites

- Snowflake account with SYSADMIN role
- Python 3.9+
- `snowflake-connector-python >= 3.0`

```bash
pip install 'snowflake-connector-python>=3.0'
```

A named connection in `~/.snowflake/connections.toml`:

```toml
[my_connection]
account = "your-account"
user = "your_user"
authenticator = "externalbrowser"    # or use password
warehouse = "COMPUTE_XS_WH"
database = "SNOW_DB"
schema = "SNOW_SCHEMA"
role = "SYSADMIN"
```

## Setup

Run `sf_setup.sql` in Snowsight or SnowSQL. This creates:

1. Database `SNOW_DB` and schema `SNOW_SCHEMA`
2. `TXN_HISTORY` — standard table, 1B rows, clustered by `CUSTOMER_ID`
3. `TXN_HISTORY_IT` — interactive table, same data
4. `COMPUTE_XS_WH` — regular XSMALL warehouse (baseline)
5. `TXN_INTERACTIVE_WH` — interactive XSMALL warehouse

```bash
snowsql -c my_connection -f sf_setup.sql
```

The 1B row INSERT takes ~15-20 minutes on an XL warehouse. The interactive table INSERT is a second pass of the same duration.

## Quick Start

```bash
# Smoke test (c=1,10,50 for 30s each)
python benchmark.py --connection my_connection

# Full benchmark (c=10-1000 for 60s each)
python benchmark.py --connection my_connection --full --procs 8
```

## Usage

### benchmark.py — Core Engine

The benchmark engine. Runs N concurrent closed-loop queries for a fixed duration, then collects server-side metrics from `QUERY_HISTORY`.

```bash
# Test IT on IWH, all queries, c=10-1000
python benchmark.py --connection my_conn --full \
    --tables IT --warehouses IWH --procs 8

# Test FDN on regular warehouse, c=10-250
python benchmark.py --connection my_conn --full \
    --tables FDN --warehouses STD \
    --levels 10 50 100 250

# Single query type
python benchmark.py --connection my_conn --full \
    --tables IT --warehouses IWH \
    --queries point_lookup --procs 8
```

**Key flags:**

| Flag | Description |
|---|---|
| `--connection` | Named connection from `connections.toml` (required) |
| `--full` | Full concurrency levels (10-1000, 60s each) |
| `--procs N` | Parallel Python processes. **Use 8+ at c>=500** to avoid GIL bottleneck |
| `--levels` | Override concurrency levels |
| `--tables` | `IT` (interactive) and/or `FDN` (standard) |
| `--warehouses` | `IWH` (interactive) and/or `STD` (regular) |
| `--queries` | `point_lookup` and/or `customer360` |
| `--duration` | Override seconds per level |

### run_suite.py — Full Suite Orchestrator

Runs the complete benchmark matrix with proper warming protocol:

```bash
# Run all 3 phases
python run_suite.py --connection my_conn

# Run only Phase 3 (regular WH baseline)
python run_suite.py --connection my_conn --phase 3

# Shorter proactive cache wait (default: 15 min)
python run_suite.py --connection my_conn --warm-wait 10
```

**Phases:**

1. **Single cluster** — IT on IWH XS, c=10 through c=500
2. **Multi-cluster** — MCW=2 at c=500 and c=1000 (procs=32)
3. **Regular WH baseline** — FDN on COMPUTE_XS_WH, c=10 through c=250

Each phase includes: full-scan warming → benchmark warming → proactive cache wait → measured run.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│ benchmark.py│────▶│  N Python procs  │────▶│  Snowflake Warehouse │
│  (main)     │     │  each with own   │     │  (IWH or Regular)    │
│             │     │  GIL + conn pool │     │                      │
│             │     │  + thread pool   │     │  QUERY_TAG = JSON    │
└─────────────┘     └──────────────────┘     └──────────┬───────────┘
                                                        │
                                              QUERY_HISTORY / ACCOUNT_USAGE
                                                        │
                                              ┌─────────▼──────────┐
                                              │  Server-side QPS,  │
                                              │  p50/p90/p99, etc. │
                                              └────────────────────┘
```

**Why multi-process?** Python's GIL serializes CPU work within a single process. At c=1000, 1000 threads competing for one GIL causes client-side contention that underreports true server throughput. Splitting across 8-32 processes (each running ~30-125 threads) eliminates this bottleneck.

**Why `fetchone()` instead of `fetchall()`?** The Customer 360 query returns ~10 rows. `fetchall()` deserializes all rows under the GIL. `fetchone()` reduces per-query client overhead, especially at high concurrency.

**Warming protocol:** Interactive Warehouses use proactive caching to load hot data into memory. The suite:
1. Runs full-scan queries to populate the cache
2. Runs a benchmark warm pass at moderate concurrency
3. Waits 15 minutes for the proactive caching system to optimize data placement
4. Then runs the measured test

## Server-Side Metrics

Client-side QPS underreports at high concurrency due to connection setup time and Python overhead. The benchmark tags every query with a JSON `QUERY_TAG`:

```json
{
    "run_id": "20260914_130140_d56e38",
    "test": "Point Lookup [IT]",
    "warehouse": "IT/IWH",
    "concurrency": 1000
}
```

Query server-side metrics via `ACCOUNT_USAGE` (available ~45 min after the run):

```sql
SELECT PARSE_JSON(query_tag):test::STRING   AS test,
       PARSE_JSON(query_tag):concurrency::INT AS conc,
       COUNT(*)                               AS n,
       ROUND(COUNT(*) / 60.0)               AS qps,
       APPROX_PERCENTILE(total_elapsed_time, 0.50) AS p50,
       APPROX_PERCENTILE(total_elapsed_time, 0.90) AS p90,
       APPROX_PERCENTILE(total_elapsed_time, 0.99) AS p99
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%YOUR_RUN_ID%'
  AND query_type = 'SELECT' AND execution_status = 'SUCCESS'
  AND start_time >= CURRENT_DATE
GROUP BY 1, 2 ORDER BY 1, 2;
```

## Files

```
iwh-benchmark/
├── sf_setup.sql     # DDL + data generation (run first)
├── benchmark.py     # Core benchmark engine
├── run_suite.py     # Full suite orchestrator
└── README.md        # This file
```

## License

MIT
