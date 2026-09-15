# IWH Concurrency Benchmark

A lightweight, portable benchmark for measuring **high-throughput query concurrency** on Snowflake Interactive Warehouses. It drives 10 to 1,000+ simultaneous queries against a 1-billion-row table and reports both client- and server-side throughput and latency.

**Blog**: [Real-Time Analytics on Snowflake: A Repeatable Benchmark for Sub-Second Latency at Scale](https://medium.com/@paul.needleman/real-time-analytics-on-snowflake-a-repeatable-benchmark-for-sub-second-latency-at-scale-49b666f2deb2)

## Why not just use JMeter?

Tools like JMeter, Gatling, or k6 are excellent general-purpose load generators, but for this specific job — pushing a database to its concurrency ceiling and reading the results back from the database's own telemetry — they add friction:

- **Portability.** This is a single Python file with one dependency (`snowflake-connector-python`). No JVM, no GUI, no `.jmx` XML to maintain, no plugin ecosystem to learn. Clone it, point it at a connection, run it. It goes wherever a laptop or a CI runner can run Python.
- **Ease of use.** Adding a query is editing a dict literal. Changing concurrency is a CLI flag. There is no test-plan abstraction to model — the thing you configure *is* the thing that runs.
- **Built for high-throughput concurrency.** The design goal is saturating the warehouse, not simulating realistic user think-time. It uses a **closed-loop** model (workers fire the next query the instant the previous one returns) spread across multiple OS processes to sidestep Python's GIL — so the client can actually generate thousands of concurrent in-flight queries instead of bottlenecking on itself.
- **Trustworthy numbers.** Every query is tagged and results are read back from Snowflake's `QUERY_HISTORY`, so the reported latency and QPS are the database's own server-side measurements — not client-side timings inflated by network round-trips and driver overhead.

It is intentionally *not* a replacement for JMeter. It is a focused instrument for one question: **how many concurrent queries can this warehouse sustain, and how fast does each one stay?**

## What It Tests

Query patterns against 1B rows (100M distinct customers):

| Query | Pattern | Rows Returned |
|---|---|---|
| **Point Lookup** | `WHERE customer_id = ?` with aggregation | 1 row |
| **Customer 360** | `WHERE customer_id = ?` with `GROUP BY` | ~10 rows |
| **Point Lookup (Email)** | reads `CUSTOMER_EMAIL` — for the masking-policy overhead test | 1 row |

Across three table types:
- **FDN** — Standard table on a regular warehouse (baseline)
- **IT** — Interactive Table on an Interactive Warehouse (the new architecture)
- **IB** — External Apache Iceberg table (**optional — requires an external catalog integration**)

The **masking-policy test** (optional) measures column-level governance overhead: run `point_lookup_email` with the `EMAIL_MASK` policy attached to `CUSTOMER_EMAIL`, then detach it and re-run, and compare throughput/latency. Setup for both IB and the masking policy is in `sf_setup.sql` (both optional).

## Results

Full results, charts, and analysis are in the [companion blog post](https://medium.com/@paul.needleman/real-time-analytics-on-snowflake-a-repeatable-benchmark-for-sub-second-latency-at-scale-49b666f2deb2).

In short: on an XSMALL Interactive Warehouse, both query patterns held **sub-30ms p50 latency at 250 concurrent queries**, where an equivalently-sized regular warehouse degraded past 600ms under queuing. With a second cluster enabled, throughput scaled past **2,000 QPS at 1,000 concurrent queries**. Run it yourself — the numbers below are reproducible with the steps in this repo.

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
2. `TXN_HISTORY` — standard table (FDN), 1B rows, clustered by `CUSTOMER_ID`
3. `TXN_HISTORY_IT` — interactive table (IT), same data
4. `COMPUTE_XS_WH` — regular XSMALL warehouse (baseline)
5. `TXN_INTERACTIVE_WH` — interactive XSMALL warehouse

It also contains **optional** sections — commented out with their prerequisites — for an external Iceberg table (IB) and the `EMAIL_MASK` masking policy (governance-overhead test). Neither is required for the core FDN/IT benchmarks; uncomment them only if you want those scenarios.

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

# Masking-policy overhead: run the email query with EMAIL_MASK attached to
# CUSTOMER_EMAIL, then UNSET it (see sf_setup.sql) and re-run to compare.
python benchmark.py --connection my_conn --full \
    --tables IT --warehouses IWH \
    --queries point_lookup_email --procs 8
```

**Key flags:**

| Flag | Description |
|---|---|
| `--connection` | Named connection from `connections.toml` (required) |
| `--full` | Full concurrency levels (10-1000, 60s each) |
| `--procs N` | Parallel Python processes. **Use 8+ at c>=500** to avoid GIL bottleneck |
| `--levels` | Override concurrency levels |
| `--tables` | `IT` (interactive), `FDN` (standard), and/or `IB` (Iceberg) |
| `--warehouses` | `IWH` (interactive) and/or `STD` (regular) |
| `--queries` | `point_lookup`, `customer360`, and/or `point_lookup_email` |
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

## Notes & Best Practices

Read this before trusting any number the tool prints.

- **Warm vs. cold cache.** All headline results are for a *warmed* warehouse. Interactive Warehouses use proactive caching; a cold warehouse will show much higher latency for the first queries. The suite's warming protocol exists precisely to remove this variable — don't compare a cold run to a warm one.
- **Clustering is not optional — it's the whole game.** The tables are `CLUSTER BY (CUSTOMER_ID)` and loaded `ORDER BY CUSTOMER_ID`. Since every benchmark query filters on `CUSTOMER_ID`, clustering lets Snowflake prune to ~1 micro-partition instead of scanning all ~1,400. Drop the clustering and point-lookup latency collapses from tens of milliseconds to full-scan territory — you'd be benchmarking a table scan, not a lookup. If you adapt the queries to filter on a different column, cluster on that column.
- **The client machine is a variable.** At high concurrency the *load generator* can become the bottleneck before the warehouse does. Results in the blog were generated from a 12-core laptop. Fewer cores, a busy machine, or high network latency to the Snowflake region will cap the concurrency you can actually generate. Run from a machine in (or near) the same cloud region for cleanest numbers, and watch client CPU during the run.
- **Trust server-side numbers over client-side.** The console prints client-side QPS for a live signal, but connection setup and Python overhead make it *underreport*. The `QUERY_HISTORY` / `ACCOUNT_USAGE` numbers are authoritative — always report those.
- **Tune `--procs` to your core count.** One process cannot generate 1,000 truly-concurrent queries because of Python's GIL. A good starting point is `--procs = physical_core_count`. At c≥500 use 8+; the blog's c=1000 runs used `--procs 32`.
- **Closed-loop, not open-loop.** Workers fire the next query immediately — this measures *maximum sustainable throughput*, not latency under a fixed arrival rate. If you need "latency at exactly 500 req/s," this is the wrong tool (use JMeter/k6 with a fixed rate).
- **Cost is not measured here.** The benchmark reports throughput and latency, not credits consumed. Compute cost per query is left as a separate analysis — factor it in before drawing price/performance conclusions.
- **Run more than once.** Cloud performance varies run-to-run. Take the median of 3+ runs before quoting a number, and keep the `run_id`s so you can audit them later in `ACCOUNT_USAGE`.
- **`sf_setup.sql` uses an XLARGE warehouse to generate 1B rows.** That is not free and the INSERT runs twice (standard + interactive table). It auto-suspends after 60s, but be aware of the credits before running it.
- **Full suite takes ~2+ hours.** Each phase includes two 15-minute cache-priming waits. Use `--warm-wait` to shorten for smoke tests, but shortening the wait changes the results.

## Porting to Another Engine (Databricks, ClickHouse, Postgres, …)

This tool is Snowflake-coupled by design, but the reusable core is engine-agnostic. If you want to adapt it:

- **Reusable as-is:** the closed-loop worker model, the multi-process fan-out to beat the GIL, and the concurrency/duration harness in `run_benchmark()`.
- **Must be swapped:**
  - `make_connection()` — replace `snowflake.connector` with the target driver. Most (`databricks-sql-connector`, `clickhouse-connect`, `psycopg2`) follow the DBAPI 2.0 `connect/cursor/execute/fetch` shape, so the worker loop needs little change.
  - **Server-side metrics** — `QUERY_TAG` + `ACCOUNT_USAGE.QUERY_HISTORY` has no direct equivalent elsewhere. You would fall back to **client-side latency** (wrap each `execute()` in `time.perf_counter()`), which every engine supports but which underreports as noted above.
  - **Session/DDL specifics** — `USE_CACHED_RESULT`, interactive-table/warehouse DDL, and the setup script are Snowflake-only.

A cross-engine comparison is only fair if each engine is configured and warmed correctly by someone who knows it well — otherwise you are benchmarking your own misconfiguration. Keep that caveat front-and-center if you publish comparative results.

## Files

```
iwh-benchmark/
├── sf_setup.sql              # DDL + data generation (run first)
├── benchmark.py              # Core benchmark engine
├── run_suite.py              # Full suite orchestrator
├── requirements.txt          # Python dependencies
├── connections.toml.example  # Connection template
└── README.md                 # This file
```
