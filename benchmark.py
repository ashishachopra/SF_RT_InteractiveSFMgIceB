#!/usr/bin/env python3
"""
IWH Concurrency Benchmark
=========================
Closed-loop benchmark for Snowflake Interactive Warehouses.

Fires N concurrent queries back-to-back for a fixed duration, measures
throughput (QPS) and latency from both client and server (QUERY_HISTORY).
Each query is tagged with a JSON QUERY_TAG for server-side recall via
ACCOUNT_USAGE.

Supports multi-process execution to avoid Python GIL bottlenecks at
high concurrency (500+ threads). Each process gets its own GIL, connection
pool, and worker threads.

Usage:
    # Quick smoke test (c=1,10,50 for 30s each)
    python benchmark.py --connection my_conn

    # Full benchmark (c=10-1000 for 60s each)
    python benchmark.py --connection my_conn --full

    # Custom levels with multi-process
    python benchmark.py --connection my_conn --levels 250 500 1000 --procs 8

    # Single query type only
    python benchmark.py --connection my_conn --full --queries point_lookup

Prerequisites:
    pip install 'snowflake-connector-python>=3.0'

    ~/.snowflake/connections.toml must contain a named connection with:
        account, user, authenticator (or password), warehouse, database, schema
"""

import argparse
import time
import random
import json
import csv
import uuid
import threading
import multiprocessing
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import snowflake.connector


# ---------------------------------------------------------------------------
# Configuration — edit these or override via CLI args
# ---------------------------------------------------------------------------

# Max connections per process. At c=1000 with procs=8, each process
# opens min(125, threads_per_proc) connections.
POOL_SIZE = 125

# Default run duration per concurrency level
RUN_DURATION_SEC = 60

# Warehouse mapping (label -> warehouse name in your account)
WAREHOUSES = {
    "IWH": "TXN_INTERACTIVE_WH",
    "STD": "COMPUTE_XS_WH",
}

# Table mapping (label -> fully qualified name)
TABLES = {
    "FDN": "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY",
    "IT":  "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT",
}


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _point_lookup_gen():
    """Random customer_id for point lookup."""
    cid = random.randint(1, 100_000_000)
    return "", (cid,)


def _customer360_gen():
    """Random customer_id for aggregation query."""
    cid = random.randint(1, 100_000_000)
    return "", (cid,)


QUERY_TEMPLATES = {
    "point_lookup": {
        "label": "Point Lookup",
        "sql": (
            "SELECT COUNT(*), SUM(UNIT_PRICE * QUANTITY) "
            "FROM {table} WHERE CUSTOMER_ID = %s"
        ),
        "gen": _point_lookup_gen,
    },
    "customer360": {
        "label": "Customer 360",
        "sql": (
            "SELECT PRODUCT_CATEGORY, STORE_ID, COUNT(*), "
            "SUM(UNIT_PRICE * QUANTITY) AS SPEND, "
            "MIN(TXN_DATE) AS FIRST_PURCHASE, MAX(TXN_DATE) AS LAST_PURCHASE "
            "FROM {table} WHERE CUSTOMER_ID = %s "
            "GROUP BY 1, 2 ORDER BY 4 DESC"
        ),
        "gen": _customer360_gen,
    },
}

MODES = {
    "test": {"concurrency": [1, 10, 50],                        "duration_sec": 30},
    "full": {"concurrency": [10, 50, 100, 250, 500, 1000],      "duration_sec": RUN_DURATION_SEC},
}


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def make_connection(connection_name, warehouse_override=None):
    """Create a connection using a named entry from connections.toml."""
    kwargs = {"connection_name": connection_name}
    if warehouse_override:
        kwargs["warehouse"] = warehouse_override
    return snowflake.connector.connect(**kwargs)


def create_pool(connection_name, warehouse, size, tag_json):
    """Create a pool of tagged connections with result cache disabled."""
    print(f"    Creating {size} connections... ", end="", flush=True)
    pool = []
    tag_str = json.dumps(tag_json).replace("'", "\\'")
    for _ in range(size):
        conn = make_connection(connection_name, warehouse_override=warehouse)
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        cur.execute(f"ALTER SESSION SET QUERY_TAG = '{tag_str}'")
        cur.close()
        pool.append(conn)
    print("done.", flush=True)
    return pool


def close_pool(pool):
    for conn in pool:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Worker: closed-loop query runner
# ---------------------------------------------------------------------------

def worker_loop(conn, query_sql, gen, stop_event, stats):
    """
    Closed-loop worker: fire a query, fetch one row, fire next.
    gen() returns (extra_clause, params) each call.
    """
    local_count = 0
    local_errors = 0

    while not stop_event.is_set():
        cur = conn.cursor()
        try:
            extra, params = gen()
            sql = query_sql.replace("{extra}", extra)
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cur.fetchone()
            local_count += 1
        except Exception:
            local_errors += 1
        finally:
            cur.close()

    with stats["lock"]:
        stats["completed"] += local_count
        stats["errors"] += local_errors


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def _run_workers(connection_name, warehouse, query_sql, gen, concurrency,
                 duration_sec, tag_json):
    """
    Single-process benchmark unit. Runs `concurrency` threads for
    `duration_sec` seconds. Returns (completed, wall_elapsed, errors).
    """
    pool_size = min(concurrency, POOL_SIZE)
    pool = create_pool(connection_name, warehouse, pool_size, tag_json)

    stats = {"completed": 0, "errors": 0, "lock": threading.Lock()}
    stop_event = threading.Event()

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(concurrency):
            conn = pool[i % pool_size]
            futures.append(
                executor.submit(worker_loop, conn, query_sql, gen,
                                stop_event, stats)
            )
        time.sleep(duration_sec)
        stop_event.set()
        for f in futures:
            f.result()
    wall_elapsed = time.perf_counter() - wall_start
    close_pool(pool)

    return stats["completed"], wall_elapsed, stats["errors"]


# Module-level reference for pickle-safe multiprocessing
_CONNECTION_NAME = None


def _proc_entry(warehouse, query_sql, qry_name, concurrency, duration_sec,
                tag_json, result_q):
    """Child-process entry point. Each process gets its own GIL."""
    gen = QUERY_TEMPLATES[qry_name]["gen"]
    completed, elapsed, errors = _run_workers(
        _CONNECTION_NAME, warehouse, query_sql, gen,
        concurrency, duration_sec, tag_json)
    result_q.put((completed, elapsed, errors))


def run_benchmark(run_id, connection_name, warehouse, tbl_label, test_name,
                  query_sql, qry_name, gen, concurrency, duration_sec, procs):
    """
    Sustained throughput test.

    When procs > 1, concurrency is divided across processes so each process
    runs concurrency // procs threads on its own GIL.
    """
    global _CONNECTION_NAME
    _CONNECTION_NAME = connection_name

    tag_json = {
        "run_id": run_id,
        "test": test_name,
        "warehouse": tbl_label,
        "concurrency": concurrency,
    }

    procs_to_use = min(procs, concurrency)
    base, rem = divmod(concurrency, procs_to_use)
    slices = [base + (1 if i < rem else 0) for i in range(procs_to_use)]

    print(f"\n    Concurrency={concurrency}, duration={duration_sec}s, "
          f"procs={procs_to_use} (threads/proc={slices})")

    if procs_to_use == 1:
        completed, wall_elapsed, errors = _run_workers(
            connection_name, warehouse, query_sql, gen,
            concurrency, duration_sec, tag_json)
    else:
        result_q = multiprocessing.Queue()
        processes = []
        for slice_conc in slices:
            p = multiprocessing.Process(
                target=_proc_entry,
                args=(warehouse, query_sql, qry_name, slice_conc,
                      duration_sec, tag_json, result_q),
            )
            p.start()
            processes.append(p)

        completed, errors, elapseds = 0, 0, []
        for _ in processes:
            c, el, er = result_q.get()
            completed += c
            errors += er
            elapseds.append(el)
        for p in processes:
            p.join()
        wall_elapsed = (max(elapseds) if elapseds
                        else time.perf_counter())

    qps = completed / wall_elapsed if wall_elapsed else 0
    print(f"    done. {completed} queries, {qps:.1f} QPS, {errors} errors",
          flush=True)

    return completed, wall_elapsed, errors


# ---------------------------------------------------------------------------
# Results collection from QUERY_HISTORY
# ---------------------------------------------------------------------------

def collect_results(connection_name, run_id):
    """Pull server-side metrics from QUERY_HISTORY."""
    print("\nCollecting server-side metrics from QUERY_HISTORY...", flush=True)
    time.sleep(3)

    conn = make_connection(connection_name)
    cur = conn.cursor()

    cur.execute(f"""
        WITH tagged AS (
            SELECT
                PARSE_JSON(query_tag) AS tag,
                total_elapsed_time,
                execution_time,
                compilation_time,
                queued_overload_time,
                bytes_scanned,
                start_time,
                end_time
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD('hours', -2, CURRENT_TIMESTAMP()),
                END_TIME_RANGE_END   => CURRENT_TIMESTAMP(),
                RESULT_LIMIT         => 10000
            ))
            WHERE query_tag LIKE '%{run_id}%'
              AND query_type = 'SELECT'
              AND execution_status = 'SUCCESS'
        )
        SELECT
            tag:test::STRING AS test,
            tag:warehouse::STRING AS warehouse,
            tag:concurrency::INT AS concurrency,
            COUNT(*) AS num_queries,
            APPROX_PERCENTILE(total_elapsed_time, 0.50) AS p50_ms,
            APPROX_PERCENTILE(total_elapsed_time, 0.90) AS p90_ms,
            APPROX_PERCENTILE(total_elapsed_time, 0.99) AS p99_ms,
            MIN(total_elapsed_time) AS min_ms,
            MAX(total_elapsed_time) AS max_ms,
            AVG(total_elapsed_time) AS avg_ms,
            AVG(execution_time) AS avg_exec_ms,
            AVG(compilation_time) AS avg_compile_ms,
            AVG(queued_overload_time) AS avg_queue_ms,
            MAX(queued_overload_time) AS max_queue_ms,
            DATEDIFF('second', MIN(start_time), MAX(end_time)) AS test_duration_sec
        FROM tagged
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()
    return columns, rows


def print_results(rows, columns):
    """Print formatted results table."""
    print(f"\n{'=' * 115}")
    print(f"{'Test':<30} {'Tbl':<6} {'Conc':>5} {'N':>6} "
          f"{'p50':>7} {'p90':>7} {'p99':>7} {'compile':>8} {'queue':>7} "
          f"{'dur_s':>6} {'QPS':>8}")
    print("-" * 115)

    for row in rows:
        (test, tbl, conc, n, p50, p90, p99,
         mn, mx, avg, avg_exec, avg_comp, avg_q, max_q, dur) = row
        qps = n / dur if dur and dur > 0 else 0
        print(f"{test:<30} {tbl:<6} {conc:>5} {n:>6} "
              f"{p50:>6.0f}ms {p90:>6.0f}ms {p99:>6.0f}ms "
              f"{avg_comp:>6.0f}ms {max_q:>6.0f}ms "
              f"{dur:>5.0f}s {qps:>7.1f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="IWH Concurrency Benchmark — closed-loop throughput test")
    parser.add_argument("--connection", required=True,
                        help="Named connection from ~/.snowflake/connections.toml")
    parser.add_argument("--full", action="store_true",
                        help="Full benchmark (c=10-1000, 60s each)")
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Tables to test (FDN, IT). Default: all")
    parser.add_argument("--warehouses", nargs="*", default=None,
                        help="Warehouses to test (IWH, STD). Default: all")
    parser.add_argument("--procs", type=int, default=1,
                        help="Parallel Python processes (default: 1). "
                             "Use 8+ at c>=500 to avoid GIL bottleneck.")
    parser.add_argument("--levels", nargs="*", type=int, default=None,
                        help="Override concurrency levels (e.g. --levels 250 500)")
    parser.add_argument("--queries", nargs="*", default=None,
                        help="Query subset (point_lookup, customer360)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Override run duration in seconds")
    args = parser.parse_args()

    mode = "full" if args.full else "test"
    config = MODES[mode]
    concurrency_levels = args.levels if args.levels else config["concurrency"]
    duration_sec = args.duration if args.duration else config["duration_sec"]

    warehouses = {k: v for k, v in WAREHOUSES.items()
                  if not args.warehouses or k in args.warehouses}
    tables = {k: v for k, v in TABLES.items()
              if not args.tables or k in args.tables}
    queries = {k: v for k, v in QUERY_TEMPLATES.items()
               if not args.queries or k in args.queries}

    run_id = (datetime.now().strftime("%Y%m%d_%H%M%S")
              + "_" + uuid.uuid4().hex[:6])

    print("=" * 80)
    print(f"IWH Concurrency Benchmark")
    print(f"Run ID:       {run_id}")
    print(f"Started:      {datetime.now().isoformat()}")
    print(f"Connection:   {args.connection}")
    print(f"Warehouses:   {list(warehouses.keys())}")
    print(f"Tables:       {list(tables.keys())}")
    print(f"Queries:      {list(queries.keys())}")
    print(f"Concurrency:  {concurrency_levels}")
    print(f"Duration:     {duration_sec}s per level")
    print(f"Pool size:    {POOL_SIZE} connections per process")
    print(f"Procs:        {args.procs}")
    print("=" * 80)

    for wh_label, wh_name in warehouses.items():
        for tbl_label, tbl_name in tables.items():
            for qry_name, qry_config in queries.items():
                test_name = f"{qry_config['label']} [{tbl_label}]"
                query_sql = qry_config["sql"].replace("{table}", tbl_name)

                print(f"\n--- {test_name} on {wh_label} ({wh_name}) ---")

                for concurrency in concurrency_levels:
                    run_benchmark(
                        run_id=run_id,
                        connection_name=args.connection,
                        warehouse=wh_name,
                        tbl_label=f"{tbl_label}/{wh_label}",
                        test_name=test_name,
                        query_sql=query_sql,
                        qry_name=qry_name,
                        gen=qry_config["gen"],
                        concurrency=concurrency,
                        duration_sec=duration_sec,
                        procs=args.procs,
                    )

    # Collect server-side metrics
    columns, rows = collect_results(args.connection, run_id)

    if not rows:
        print("\nWARNING: No results in QUERY_HISTORY yet.")
        print("  INFORMATION_SCHEMA has a ~45s lag; ACCOUNT_USAGE has ~45 min.")
        print(f"  Re-query with: query_tag LIKE '%{run_id}%'")
    else:
        print_results(rows, columns)

        csv_file = f"benchmark_{run_id}.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
        print(f"\nCSV saved: {csv_file}")

    print(f"\nServer-side recall (ACCOUNT_USAGE, ~45 min lag):")
    print(f"""
SELECT PARSE_JSON(query_tag):test::STRING   AS test,
       PARSE_JSON(query_tag):concurrency::INT AS conc,
       COUNT(*)                               AS n,
       ROUND(COUNT(*) / 60.0)                AS qps,
       APPROX_PERCENTILE(total_elapsed_time, 0.50) AS p50,
       APPROX_PERCENTILE(total_elapsed_time, 0.90) AS p90,
       APPROX_PERCENTILE(total_elapsed_time, 0.99) AS p99
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE '%{run_id}%'
  AND query_type = 'SELECT' AND execution_status = 'SUCCESS'
  AND start_time >= CURRENT_DATE
GROUP BY 1, 2 ORDER BY 1, 2;
""")

    print(f"Done. {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
