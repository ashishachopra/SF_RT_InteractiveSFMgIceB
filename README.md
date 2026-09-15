# IWH Concurrency Benchmark

A lightweight, portable benchmark for measuring **high-throughput query concurrency** on Snowflake Interactive Warehouses. It drives 10 to 1,000+ simultaneous queries against a 1-billion-row table and reports both client- and server-side throughput and latency.

**Blog**: [Real-Time Analytics on Snowflake: A Repeatable Benchmark for Sub-Second Latency at Scale](https://medium.com/@paul.needleman/real-time-analytics-on-snowflake-a-repeatable-benchmark-for-sub-second-latency-at-scale-49b666f2deb2)

> **Disclaimer.** This is an informal, self-run benchmark shared to be reproduced, not an official or audited Snowflake result and not a TPC-style certified benchmark. The views here are my own. All numbers were measured on a specific account, region, warehouse size, and client machine, and **your results will vary** with any of those. The value of this repo is the method and the fact that you can run it yourself — treat the figures as illustrative, and reproduce them in your own environment before drawing conclusions. Snowflake and Interactive Warehouse are trademarks of Snowflake Inc.

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

The 1B-row load is fast on an XL warehouse — typically a minute or two — and runs twice (standard table, then interactive table). Smaller or larger warehouses scale accordingly.

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

## Customizing — Bring Your Own Queries

This is meant to be a **flexible harness, not a fixed test**. The two shipped queries are just examples of a selective-lookup pattern — swap in your own workload and the whole concurrency/latency/QPS machinery works unchanged.

Queries live in the `QUERY_TEMPLATES` dict in `benchmark.py`. Each entry has three parts:

```python
QUERY_TEMPLATES = {
    "my_query": {                          # <- the name you pass to --queries
        "label": "My Query",               # <- shows up in output + QUERY_TAG
        "sql": "SELECT ... FROM {table} WHERE some_col = %s {extra}",
        "gen": _my_gen,                    # <- returns (extra_clause, params) per call
    },
}
```

- **`{table}`** is substituted with the fully-qualified table name at runtime, so one template runs against FDN, IT, or IB without edits.
- **`%s`** placeholders are bound safely from the `params` tuple your `gen()` returns (never string-formatted — avoids SQL injection and lets Snowflake cache the plan).
- **`gen()`** is called once per query execution and returns `(extra_clause, params)`:
  - `params` — the bind values (e.g. a random id) so every execution hits different data and you're not just re-serving one cached row.
  - `extra_clause` — optional text spliced in at `{extra}` for cases where you need to vary structure, not just values (e.g. a random `LIMIT` or an added predicate). Return `""` if unused.

Example — a random date-range scan:

```python
def _date_range_gen():
    start = random.randint(1, 2000)                 # days ago
    span  = random.choice([7, 30, 90])
    return "", (start, start - span)                # two bind params

QUERY_TEMPLATES["recent_window"] = {
    "label": "Recent Window",
    "sql": ("SELECT STORE_STATE_CD, SUM(UNIT_PRICE*QUANTITY) FROM {table} "
            "WHERE TXN_DATE BETWEEN DATEADD('day', -%s, CURRENT_DATE) "
            "AND DATEADD('day', -%s, CURRENT_DATE) GROUP BY 1"),
    "gen": _date_range_gen,
}
```

Then run just your query:

```bash
python benchmark.py --connection my_conn --full \
    --tables IT --warehouses IWH --queries recent_window --procs 8
```

Two things to keep in mind when you bring your own queries:
- **Cluster the table on whatever column your query filters on** (see best practices) so pruning reflects a real production design.
- **Randomize the bind values** in `gen()` — if every execution requests the same key, you're benchmarking the result cache, not the engine (the harness already sets `USE_CACHED_RESULT = FALSE`, but reusing one key still hits warm micro-partitions unrealistically).

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

> **Important — the in-script summary is only a sample at high concurrency.**
> After each run the script prints a server-side table pulled from
> `INFORMATION_SCHEMA.QUERY_HISTORY`, which is capped at 10,000 rows per call.
> A single `c=1000` level produces 100,000+ queries in 60s, so that table
> reflects only the most recent ~10k and its QPS/percentiles will be
> **understated**. Treat the in-script numbers as a live sanity check; use the
> `ACCOUNT_USAGE` query above (no row cap) for the authoritative figures you
> publish. `ACCOUNT_USAGE` lags ~45 min; `INFORMATION_SCHEMA` is near-real-time
> but capped.

### A note on fair comparison

The suite deliberately warms the Interactive Warehouse (full-scan + benchmark warm + a proactive-cache wait) before measuring, because proactive caching is part of how it's meant to run. The regular-warehouse baseline (Phase 3) is **not** given the same warm-up. This is intentional — a regular warehouse has no proactive cache to prime — but it means the first queries in the baseline pay compilation and cold-scan costs. If you want a strictly warm-vs-warm comparison, add a warm-up pass before Phase 3 and note it in your results.

## Notes & Best Practices

Read this before trusting any number the tool prints.

- **Warm vs. cold cache.** All headline results are for a *warmed* warehouse. Interactive Warehouses use proactive caching; a cold warehouse will show much higher latency for the first queries. The suite's warming protocol exists precisely to remove this variable — don't compare a cold run to a warm one.
- **Clustering follows Snowflake best practice.** The tables are `CLUSTER BY (CUSTOMER_ID)` and loaded `ORDER BY CUSTOMER_ID` because the workload is selective lookups on `CUSTOMER_ID` — and aligning the clustering key with the filter column is the standard way to design any high-selectivity table on Snowflake, interactive or not. It lets the engine prune to the handful of micro-partitions that hold a customer's rows instead of scanning the whole table. This isn't specific to the benchmark; it's how you'd model this table in production. If you adapt the queries to filter on a different column, cluster on that column so the same pruning applies. (Skip clustering entirely and you'd be measuring a full-table scan, which is a different workload than the low-latency lookups this benchmark targets.)
- **The client machine is a variable.** At high concurrency the *load generator* can become the bottleneck before the warehouse does. Results in the blog were generated from a 12-core laptop. Fewer cores, a busy machine, or high network latency to the Snowflake region will cap the concurrency you can actually generate. Run from a machine in (or near) the same cloud region for cleanest numbers, and watch client CPU during the run.
- **Reported numbers are server-side execution time, not client wall-clock.** The authoritative latency/QPS come from `QUERY_HISTORY` (`total_elapsed_time`), i.e. time measured inside Snowflake. Client-observed timing includes network round-trips, TLS, and driver deserialization — so a client far from the Snowflake region will see slower *end-to-end* times even though server execution is identical. The console prints a client-side QPS as a live signal, but it under-reports for exactly this reason; always publish the server-side figures. Run the load generator in (or near) the same cloud region to keep client overhead from dominating.
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
