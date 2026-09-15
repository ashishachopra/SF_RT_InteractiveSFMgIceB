#!/usr/bin/env python3
"""
IWH Benchmark Suite Orchestrator
=================================
Runs the full benchmark matrix with proper warming, cache priming, and
warehouse configuration. Designed to be unattended — logs everything and
prints a summary with run_ids for ACCOUNT_USAGE recall.

Suite phases:
  Phase 1: Interactive Table on Interactive Warehouse (single cluster)
           - Full-scan warm → benchmark warm → 15 min proactive cache wait
           - Measured: c=10, 50, 100, 250, 500

  Phase 2: Multi-cluster warehouse (MCW=2) at high concurrency
           - Full-scan warm both clusters → benchmark warm → 15 min wait
           - Measured: c=500, 1000 (procs=32 for c=1000)

  Phase 3: Regular warehouse baseline
           - FDN table on COMPUTE_XS_WH, c=10, 50, 100, 250

Usage:
    python run_suite.py --connection my_conn

    # Skip phases (e.g., only run regular WH baseline)
    python run_suite.py --connection my_conn --phase 3

    # Custom proactive cache wait time
    python run_suite.py --connection my_conn --warm-wait 10
"""

import argparse
import subprocess
import sys
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import snowflake.connector

# ---------------------------------------------------------------------------
# Config — edit these to match your account
# ---------------------------------------------------------------------------
IWH = "TXN_INTERACTIVE_WH"
STD_WH = "COMPUTE_XS_WH"
IT_FQN = "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT"

BENCHMARK_SCRIPT = str(Path(__file__).parent / "benchmark.py")
LOG_DIR = Path("/tmp/iwh_benchmark_logs")

WARM_SQL = (
    "SELECT SUM(UNIT_PRICE*QUANTITY), COUNT(CUSTOMER_ID), "
    "COUNT(STORE_STATE_CD), COUNT(PRODUCT_CATEGORY), "
    "COUNT(STORE_ID), MIN(TXN_DATE) FROM {table}"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn(connection_name, warehouse=None):
    kwargs = {"connection_name": connection_name}
    if warehouse:
        kwargs["warehouse"] = warehouse
    return snowflake.connector.connect(**kwargs)


def run_sql(connection_name, warehouse, *stmts):
    """Execute SQL statements, logging each one."""
    conn = get_conn(connection_name, warehouse)
    cur = conn.cursor()
    for sql in stmts:
        print(f"  SQL: {sql[:120]}", flush=True)
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"  SQL ERROR (non-fatal): {e}", flush=True)
    conn.close()


def full_scan_warm(connection_name, table_fqn, warehouse,
                   attempts=3, concurrent=8):
    """Warm the cache with concurrent full-scan queries."""
    sql = WARM_SQL.format(table=table_fqn)
    print(f"  Full-scan warm: {concurrent} concurrent × {attempts} rounds",
          flush=True)

    def run_one(_):
        try:
            conn = get_conn(connection_name, warehouse)
            cur = conn.cursor()
            cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
            cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600")
            cur.execute(sql)
            cur.fetchall()
            conn.close()
            return "ok"
        except Exception as e:
            print(f"    WARM ERROR: {e}", flush=True)
            return "fail"

    for r in range(attempts):
        with ThreadPoolExecutor(max_workers=concurrent) as ex:
            results = list(ex.map(run_one, range(concurrent)))
        ok = sum(1 for x in results if x == "ok")
        print(f"    Round {r+1}: {ok}/{concurrent} ok", flush=True)


def run_benchmark(connection_name, table_key, wh_key, levels,
                  log_suffix, queries=None, procs=8):
    """Launch the benchmark subprocess and extract the run_id."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"suite_{log_suffix}.log"

    if queries is None:
        queries = ["point_lookup", "customer360"]

    cmd = [
        sys.executable, "-u", BENCHMARK_SCRIPT,
        "--connection", connection_name,
        "--full", "--procs", str(procs),
        "--tables", table_key,
        "--warehouses", wh_key,
        "--queries", *queries,
        "--levels", *[str(l) for l in levels],
    ]

    print(f"  Benchmark: table={table_key} wh={wh_key} levels={levels} "
          f"procs={procs}", flush=True)
    print(f"  Log: {log_file}", flush=True)

    with open(log_file, "w") as f:
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)

    run_id = "unknown"
    with open(log_file) as f:
        for line in f:
            if "Run ID:" in line:
                run_id = line.split("Run ID:")[1].strip()
                break

    with open(log_file) as f:
        for line in f:
            line = line.strip()
            if "done." in line and "queries" in line:
                print(f"    {line}", flush=True)

    return run_id, str(log_file)


def wait_min(minutes, label):
    """Wait with progress updates."""
    print(f"  Waiting {minutes} min for {label}...", flush=True)
    for i in range(minutes):
        time.sleep(60)
        print(f"    {i+1}/{minutes} min", flush=True)


# ---------------------------------------------------------------------------
# Suite phases
# ---------------------------------------------------------------------------

def phase1_single_cluster(connection_name, warm_wait_min, summary):
    """IT on IWH single cluster, c=10 through c=500."""
    print(f"\n{'='*70}")
    print("PHASE 1: Interactive Table — Single Cluster (XS)")
    print(f"{'='*70}", flush=True)

    # Resume and configure
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} RESUME IF SUSPENDED",
            f"ALTER WAREHOUSE {IWH} SET MAX_CONCURRENCY_LEVEL = 32",
            f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
            f"MAX_CLUSTER_COUNT = 1")

    # Ensure IT is attached
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} ADD TABLES ({IT_FQN})")

    # Full-scan warm
    print("\n[1] Full-scan warm...", flush=True)
    full_scan_warm(connection_name, IT_FQN, IWH, attempts=3, concurrent=4)

    # Benchmark warm at c=250
    print("\n[2] Benchmark warm (c=250)...", flush=True)
    run_benchmark(connection_name, "IT", "IWH", [250], "p1_warm")

    # Proactive cache wait
    print(f"\n[3] Proactive cache wait ({warm_wait_min} min)...", flush=True)
    wait_min(warm_wait_min, "proactive caching")

    # Measured run
    print("\n[4] Measured: c=10, 50, 100, 250, 500...", flush=True)
    rid, log = run_benchmark(connection_name, "IT", "IWH",
                             [10, 50, 100, 250, 500], "p1_measured")
    summary.append({
        "phase": 1, "config": "IT on IWH XS, 1 cluster, MCL=32",
        "run_id": rid, "log": log,
    })


def phase2_mcw(connection_name, warm_wait_min, summary):
    """IT on IWH with MCW=2 at c=500, c=1000."""
    print(f"\n{'='*70}")
    print("PHASE 2: Multi-Cluster Warehouse (MCW=2)")
    print(f"{'='*70}", flush=True)

    # Self-contained setup so this phase can run standalone (--phase 2):
    # resume, attach IT, set MCL=32, then scale to 2 clusters.
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} RESUME IF SUSPENDED",
            f"ALTER WAREHOUSE {IWH} ADD TABLES ({IT_FQN})",
            f"ALTER WAREHOUSE {IWH} SET MAX_CONCURRENCY_LEVEL = 32",
            f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 2, "
            f"MAX_CLUSTER_COUNT = 2")

    # Full-scan warm both clusters
    print("\n[1] Full-scan warm both clusters...", flush=True)
    full_scan_warm(connection_name, IT_FQN, IWH, attempts=3, concurrent=8)

    # Benchmark warm c=500
    print("\n[2] Benchmark warm (c=500)...", flush=True)
    run_benchmark(connection_name, "IT", "IWH", [500], "p2_warm")

    # Proactive cache wait
    print(f"\n[3] Proactive cache wait ({warm_wait_min} min)...", flush=True)
    wait_min(warm_wait_min, "proactive caching")

    # Measured c=500
    print("\n[4] Measured: c=500...", flush=True)
    rid, log = run_benchmark(connection_name, "IT", "IWH", [500],
                             "p2_c500", procs=8)
    summary.append({
        "phase": 2, "config": "IT on IWH XS, MCW=2, MCL=32, c=500",
        "run_id": rid, "log": log,
    })

    # Measured c=1000 (procs=32 to avoid GIL bottleneck)
    print("\n[5] Measured: c=1000 (procs=32)...", flush=True)
    rid, log = run_benchmark(connection_name, "IT", "IWH", [1000],
                             "p2_c1000", procs=32)
    summary.append({
        "phase": 2, "config": "IT on IWH XS, MCW=2, MCL=32, c=1000",
        "run_id": rid, "log": log,
    })

    # Revert to single cluster
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
            f"MAX_CLUSTER_COUNT = 1")


def phase3_regular_wh(connection_name, summary):
    """FDN table on regular warehouse, c=10 through c=250."""
    print(f"\n{'='*70}")
    print("PHASE 3: Regular Warehouse Baseline (FDN on COMPUTE_XS_WH)")
    print(f"{'='*70}", flush=True)

    rid, log = run_benchmark(connection_name, "FDN", "STD",
                             [10, 50, 100, 250], "p3_regular_wh")
    summary.append({
        "phase": 3, "config": "FDN on Regular XS WH",
        "run_id": rid, "log": log,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="IWH Benchmark Suite — full test matrix with warming")
    parser.add_argument("--connection", required=True,
                        help="Named connection from connections.toml")
    parser.add_argument("--phase", type=int, nargs="*", default=[1, 2, 3],
                        help="Phases to run (default: 1 2 3)")
    parser.add_argument("--warm-wait", type=int, default=15,
                        help="Proactive cache wait in minutes (default: 15)")
    args = parser.parse_args()

    suite_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary = []

    print("=" * 70)
    print(f"IWH BENCHMARK SUITE — {suite_id}")
    print(f"Started:    {datetime.now().isoformat()}")
    print(f"Connection: {args.connection}")
    print(f"Phases:     {args.phase}")
    print(f"Warm wait:  {args.warm_wait} min")
    print("=" * 70, flush=True)

    # Wrap the run so that, no matter how a phase fails, we always revert the
    # warehouse to 1 cluster and suspend it. The IWH has a 24h auto-suspend,
    # so a crash mid-run could otherwise leave a multi-cluster warehouse
    # running (and billing) for a long time.
    try:
        if 1 in args.phase:
            phase1_single_cluster(args.connection, args.warm_wait, summary)

        if 2 in args.phase:
            phase2_mcw(args.connection, args.warm_wait, summary)

        if 3 in args.phase:
            phase3_regular_wh(args.connection, summary)
    finally:
        print("\nReverting to 1 cluster and suspending IWH...", flush=True)
        run_sql(args.connection, STD_WH,
                f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
                f"MAX_CLUSTER_COUNT = 1",
                f"ALTER WAREHOUSE {IWH} SUSPEND")

    # Summary
    summary_file = LOG_DIR / f"suite_{suite_id}_summary.json"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"SUITE COMPLETE: {suite_id}")
    print(f"Finished: {datetime.now().isoformat()}")
    print(f"\nRun IDs (use with ACCOUNT_USAGE after ~45 min):")
    for s in summary:
        print(f"  Phase {s['phase']} | {s['config']}")
        print(f"           run_id: {s['run_id']}")

    conditions = " OR ".join(
        f"query_tag LIKE '%{s['run_id']}%'" for s in summary)
    print(f"""
ACCOUNT_USAGE recall query:

SELECT PARSE_JSON(query_tag):run_id::STRING   AS run_id,
       PARSE_JSON(query_tag):test::STRING     AS test,
       PARSE_JSON(query_tag):concurrency::INT AS conc,
       COUNT(*)                               AS n,
       ROUND(COUNT(*) / 60.0)                AS qps,
       APPROX_PERCENTILE(total_elapsed_time, 0.50) AS p50,
       APPROX_PERCENTILE(total_elapsed_time, 0.90) AS p90,
       APPROX_PERCENTILE(total_elapsed_time, 0.99) AS p99,
       AVG(queued_overload_time) AS avg_queue_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE ({conditions})
  AND query_type = 'SELECT' AND execution_status = 'SUCCESS'
  AND start_time >= CURRENT_DATE
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
""")

    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary saved: {summary_file}")
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
