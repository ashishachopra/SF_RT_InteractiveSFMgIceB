# Interactive Warehouse Concurrency Benchmark

A lightweight, portable benchmark for measuring **high-throughput query concurrency** on Snowflake Interactive Warehouses. It drives 10 to 1,000+ simultaneous queries against a 1-billion-row table and reports throughput and latency.

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
In short: on an XSMALL Interactive Warehouse, both query patterns held **sub-30ms p50 latency at 250 concurrent queries**, where an equivalently-sized regular warehouse degraded past 600ms under queuing. With a second cluster enabled, throughput scaled past **2,000 QPS at 1,000 concurrent queries**. Run it yourself — the numbers below are reproducible with the steps in this repo.

**Phases:**

- **A — Single cluster** (MIN=MAX=1, XSMALL): all tables, measured at c=10, 50, 100, 250
- **B — Multi-cluster** (MIN=MAX=2): all tables, measured at c=500, 1000
- **C — Regular WH baseline** (FDN on `COMPUTE_XS_WH`, c=10–250): **off by default**, enable with `--include-baseline`

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

> **Important — the in-script summary is only a sample at high concurrency.**
> After each run the script prints a server-side table pulled from
> `INFORMATION_SCHEMA.QUERY_HISTORY`, which is capped at 10,000 rows per call.
> A single `c=1000` level produces 100,000+ queries in 60s, so that table
> reflects only the most recent ~10k and its QPS/percentiles will be
> **understated**. Treat the in-script numbers as a live sanity check; use the
> `ACCOUNT_USAGE` query above (no row cap) for the authoritative figures you
> publish. `ACCOUNT_USAGE` lags ~45 min; `INFORMATION_SCHEMA` is near-real-time
> but capped.

### Output files

Each `benchmark.py` run writes up to two CSVs, named by `run_id`:

| File | Source | Contents |
|---|---|---|
| `benchmark_<run_id>.csv` | `QUERY_HISTORY` (server-side) | The authoritative per-level metrics — QPS, p50/p90/p99, counts. **This is what you publish.** |
| `client_latency_<run_id>.csv` | client wall-clock (`perf_counter`) | Per-level client-observed latency: `test, table, warehouse, concurrency, client_p50_ms, client_p90_ms, client_p99_ms, n` |

The **client-side wall-clock** capture times each query end-to-end on the client — from just before `execute()` to just after the fetch — in a thread-local list per worker, merged after each level. The console prints a `client-side (wall-clock): p50/p90/p99` line per level, and the full breakdown lands in `client_latency_<run_id>.csv`.

This is the one place the tool reports **end-to-end** latency (network round-trip + TLS + driver deserialization + server execution), so it's the number to use when you care about *what the application actually experiences* — e.g. quantifying the network tax between your client region and the Snowflake region. It is **not** a substitute for the server-side figures: at high concurrency the client is a variable (GIL, cores, network), so wall-clock over-reports latency and under-reports QPS. Use `benchmark_<run_id>.csv` / `ACCOUNT_USAGE` for headline numbers and `client_latency_<run_id>.csv` to characterize the client/network overhead on top.

## Porting to Another Engine (Databricks, ClickHouse, Postgres, …)

This tool is Snowflake-coupled by design, but the reusable core is engine-agnostic. If you want to adapt it:

- **Reusable as-is:** the closed-loop worker model, the multi-process fan-out to beat the GIL, and the concurrency/duration harness in `run_benchmark()`.
- **Must be swapped:**
  - `make_connection()` — replace `snowflake.connector` with the target driver. Most (`databricks-sql-connector`, `clickhouse-connect`, `psycopg2`) follow the DBAPI 2.0 `connect/cursor/execute/fetch` shape, so the worker loop needs little change.
  - **Server-side metrics** — `QUERY_TAG` + `ACCOUNT_USAGE.QUERY_HISTORY` has no direct equivalent elsewhere. You would fall back to **client-side latency** (wrap each `execute()` in `time.perf_counter()`), which every engine supports but which underreports as noted above.
  - **Session/DDL specifics** — `USE_CACHED_RESULT`, interactive-table/warehouse DDL, and the setup script are Snowflake-only.

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
