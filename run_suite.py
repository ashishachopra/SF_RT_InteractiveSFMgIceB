#!/usr/bin/env python3
"""
IWH Benchmark Suite Orchestrator
=================================
Runs the full benchmark matrix with proper warming, cache priming, and
warehouse configuration. Designed to be unattended — logs everything and
prints a summary with run_ids for ACCOUNT_USAGE recall.

Tests all three table types on the Interactive Warehouse via Zero-Copy
Interactive (standard/Iceberg tables query directly, no Interactive Table
required):
  FDN = standard table       (SNOW_DB.SNOW_SCHEMA.TXN_HISTORY)
  IT  = interactive table    (SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT)
  IB  = external Iceberg tbl  (ICE_DB_CLD."hassium_db"."txn_history_IB")

Suite phases:
  Phase A: Single cluster (MIN=MAX=1), measured at c=10, 50, 100, 250
  Phase B: Multi-cluster  (MIN=MAX=2), measured at c=500, 1000

Each phase loops over all three tables. For every table it attaches the
table, full-scan warms, benchmark warms, waits for the proactive cache, and
then VERIFIES the actual started_clusters count (SHOW WAREHOUSES) before the
measured run — aborting rather than risk a run contaminated by a leftover
cluster. The verified cluster count is stamped into each run's QUERY_TAG.

Usage:
    python run_suite.py --connection my_conn

    # Only one phase
    python run_suite.py --connection my_conn --phase A

    # Also refresh the regular-WH baseline (off by default)
    python run_suite.py --connection my_conn --include-baseline

    # Shorter proactive cache wait
    python run_suite.py --connection my_conn --warm-wait 5
"""

import argparse
import os
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

# Table label -> fully qualified name. IT is the materialized interactive
# table; FDN and IB are queried directly via Zero-Copy Interactive.
TABLES = [
    ("IT",  "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY_IT"),
    ("FDN", "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY"),
    ("IB",  'ICE_DB_CLD."hassium_db"."txn_history_IB"'),
]
FDN_FQN = "SNOW_DB.SNOW_SCHEMA.TXN_HISTORY"

# Every run in this suite uses 16 processes and the two non-masking queries.
PROCS = 16
QUERIES = ["point_lookup", "customer360"]

SINGLE_CLUSTER_LEVELS = [10, 50, 100, 250]
MCW_LEVELS = [500, 1000]

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
    # Portability escape hatch: if SNOWFLAKE_PAT_FILE is set, use its token
    # for password auth (the toml still supplies account/user/host/etc.).
    pat_file = os.environ.get("SNOWFLAKE_PAT_FILE")
    if pat_file:
        with open(pat_file) as f:
            kwargs["password"] = f.read().strip()
        kwargs["authenticator"] = "snowflake"
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


def get_started_clusters(connection_name, warehouse):
    """Return the real-time started_clusters count from SHOW WAREHOUSES.

    Unlike WAREHOUSE_EVENTS_HISTORY / ACCOUNT_USAGE (which lag), SHOW
    WAREHOUSES reflects the current running-cluster count immediately.
    """
    conn = get_conn(connection_name, STD_WH)
    cur = conn.cursor()
    cur.execute(f"SHOW WAREHOUSES LIKE '{warehouse}'")
    cols = [d[0].lower() for d in cur.description]
    row = cur.fetchone()
    conn.close()
    return int(dict(zip(cols, row))["started_clusters"])


def wait_for_state(connection_name, warehouse, target, timeout_min=5):
    """Poll SHOW WAREHOUSES until the warehouse reaches `target` state.

    SUSPEND/RESUME are async: right after issuing SUSPEND the warehouse is
    still 'SUSPENDING' (quiescing), and a resize issued in that window fails
    with 'Unable to perform warehouse operation'. Poll the real state first.
    """
    deadline = time.time() + timeout_min * 60
    state = None
    while time.time() < deadline:
        conn = get_conn(connection_name, STD_WH)
        cur = conn.cursor()
        cur.execute(f"SHOW WAREHOUSES LIKE '{warehouse}'")
        cols = [d[0].lower() for d in cur.description]
        state = dict(zip(cols, cur.fetchone()))["state"]
        conn.close()
        if state == target:
            print(f"  Warehouse state = {state}", flush=True)
            return
        print(f"  Warehouse state = {state}, waiting for {target}...",
              flush=True)
        time.sleep(10)
    raise RuntimeError(
        f"Warehouse never reached state {target} after {timeout_min} min "
        f"(last: {state}).")


def verify_cluster_count(connection_name, warehouse, expected, timeout_min=5):
    """Poll started_clusters until it equals `expected`, or hard-fail.

    This is the guard against the contamination bug where a leftover cluster
    from a prior phase silently inflated a run tagged as "single cluster".
    We abort the run rather than measure against the wrong cluster count.
    """
    deadline = time.time() + timeout_min * 60
    actual = None
    while time.time() < deadline:
        actual = get_started_clusters(connection_name, warehouse)
        if actual == expected:
            print(f"  Verified started_clusters = {actual}", flush=True)
            return actual
        print(f"  started_clusters = {actual}, waiting for {expected}...",
              flush=True)
        time.sleep(20)
    raise RuntimeError(
        f"started_clusters never reached {expected} after {timeout_min} min "
        f"(last seen: {actual}). Aborting to avoid a contaminated run."
    )


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
                  log_suffix, queries=None, procs=PROCS, tag_extra=None):
    """Launch the benchmark subprocess and extract the run_id."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"suite_{log_suffix}.log"

    if queries is None:
        queries = QUERIES

    cmd = [
        sys.executable, "-u", BENCHMARK_SCRIPT,
        "--connection", connection_name,
        "--full", "--procs", str(procs),
        "--tables", table_key,
        "--warehouses", wh_key,
        "--queries", *queries,
        "--levels", *[str(l) for l in levels],
    ]
    if tag_extra:
        cmd += ["--tag-extra", *[f"{k}={v}" for k, v in tag_extra.items()]]

    print(f"  Benchmark: table={table_key} wh={wh_key} levels={levels} "
          f"procs={procs} tag_extra={tag_extra}", flush=True)
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

def phase_single_cluster(connection_name, warm_wait_min, summary,
                         tables=TABLES, queries=None, extra_tag=None):
    """All 3 tables on the IWH, single cluster (XS), c=10..250."""
    print(f"\n{'='*70}")
    print("PHASE A: Single Cluster (XS) — FDN, IT, IB")
    print(f"{'='*70}", flush=True)

    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} RESUME IF SUSPENDED",
            f"ALTER WAREHOUSE {IWH} SET MAX_CONCURRENCY_LEVEL = 32",
            f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
            f"MAX_CLUSTER_COUNT = 1")
    verify_cluster_count(connection_name, IWH, 1)

    for tbl_key, tbl_fqn in tables:
        print(f"\n--- Phase A: {tbl_key} ({tbl_fqn}) ---", flush=True)

        run_sql(connection_name, STD_WH,
                f"ALTER WAREHOUSE {IWH} ADD TABLES ({tbl_fqn})")

        print(f"\n[A.1 {tbl_key}] Full-scan warm...", flush=True)
        full_scan_warm(connection_name, tbl_fqn, IWH, attempts=3, concurrent=4)

        print(f"\n[A.2 {tbl_key}] Benchmark warm (c=250)...", flush=True)
        run_benchmark(connection_name, tbl_key, "IWH", [250],
                      f"pA_{tbl_key}_warm", queries=queries)

        print(f"\n[A.3 {tbl_key}] Proactive cache wait "
              f"({warm_wait_min} min)...", flush=True)
        wait_min(warm_wait_min, f"{tbl_key} proactive caching")

        # Re-verify right before measuring, in case anything drifted.
        verify_cluster_count(connection_name, IWH, 1)

        print(f"\n[A.4 {tbl_key}] Measured: {SINGLE_CLUSTER_LEVELS}...",
              flush=True)
        a_tag = {"started_clusters": 1, "warehouse_size": "XSMALL"}
        if extra_tag:
            a_tag.update(extra_tag)
        rid, log = run_benchmark(
            connection_name, tbl_key, "IWH", SINGLE_CLUSTER_LEVELS,
            f"pA_{tbl_key}_measured", queries=queries, tag_extra=a_tag)
        summary.append({
            "phase": "A", "table": tbl_key,
            "config": f"{tbl_key} on IWH XS, 1 cluster, MCL=32",
            "run_id": rid, "log": log,
        })


def phase_mcw(connection_name, warm_wait_min, summary, size="XSMALL",
              tables=TABLES, mcw_levels=MCW_LEVELS, queries=None,
              extra_tag=None):
    """All 3 tables on the IWH, MCW=2, at the given size and levels.

    If mcw_levels includes a low warm-up level (e.g. 250) as its first entry,
    each query self-warms before its measured levels — useful for Iceberg,
    which has no local cache and otherwise penalizes whichever query runs
    second in the pass.
    """
    print(f"\n{'='*70}")
    print(f"PHASE B: Multi-Cluster Warehouse (MCW=2, {size}) — FDN, IT, IB")
    print(f"{'='*70}", flush=True)

    # Interactive warehouses can't be resized while running, so suspend
    # first, WAIT for it to actually reach SUSPENDED (suspend is async), then
    # set the size and resume. Setting XSMALL when already XSMALL is a no-op.
    run_sql(connection_name, STD_WH, f"ALTER WAREHOUSE {IWH} SUSPEND")
    wait_for_state(connection_name, IWH, "SUSPENDED")
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} SET WAREHOUSE_SIZE = {size}")
    run_sql(connection_name, STD_WH,
            f"ALTER WAREHOUSE {IWH} RESUME",
            f"ALTER WAREHOUSE {IWH} SET MAX_CONCURRENCY_LEVEL = 32",
            f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 2, "
            f"MAX_CLUSTER_COUNT = 2")
    verify_cluster_count(connection_name, IWH, 2)

    try:
        for tbl_key, tbl_fqn in tables:
            print(f"\n--- Phase B: {tbl_key} ({tbl_fqn}) ---", flush=True)

            run_sql(connection_name, STD_WH,
                    f"ALTER WAREHOUSE {IWH} ADD TABLES ({tbl_fqn})")

            print(f"\n[B.1 {tbl_key}] Full-scan warm both clusters...",
                  flush=True)
            full_scan_warm(connection_name, tbl_fqn, IWH,
                           attempts=3, concurrent=8)

            print(f"\n[B.2 {tbl_key}] Benchmark warm (c=500)...", flush=True)
            run_benchmark(connection_name, tbl_key, "IWH", [500],
                          f"pB_{tbl_key}_warm", queries=queries)

            print(f"\n[B.3 {tbl_key}] Proactive cache wait "
                  f"({warm_wait_min} min)...", flush=True)
            wait_min(warm_wait_min, f"{tbl_key} proactive caching")

            verify_cluster_count(connection_name, IWH, 2)

            print(f"\n[B.4 {tbl_key}] Measured: {mcw_levels}...", flush=True)
            b_tag = {"started_clusters": 2, "warehouse_size": size}
            if extra_tag:
                b_tag.update(extra_tag)
            rid, log = run_benchmark(
                connection_name, tbl_key, "IWH", mcw_levels,
                f"pB_{tbl_key}_measured", queries=queries, tag_extra=b_tag)
            summary.append({
                "phase": "B", "table": tbl_key,
                "config": f"{tbl_key} on IWH {size}, MCW=2, MCL=32",
                "run_id": rid, "log": log,
            })
    finally:
        # Always drop back to a single cluster and verify it actually
        # happened, so a stuck 2nd cluster can't contaminate a later run.
        run_sql(connection_name, STD_WH,
                f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
                f"MAX_CLUSTER_COUNT = 1")
        try:
            verify_cluster_count(connection_name, IWH, 1)
        except RuntimeError as e:
            print(f"  WARNING: {e}", flush=True)


def phase_regular_baseline(connection_name, summary):
    """FDN table on the regular XS warehouse, c=10..250. Off by default."""
    print(f"\n{'='*70}")
    print("PHASE C: Regular Warehouse Baseline (FDN on COMPUTE_XS_WH)")
    print(f"{'='*70}", flush=True)

    rid, log = run_benchmark(connection_name, "FDN", "STD",
                             SINGLE_CLUSTER_LEVELS, "pC_regular_wh",
                             tag_extra={"started_clusters": 1,
                                        "warehouse_size": "XSMALL"})
    summary.append({
        "phase": "C", "table": "FDN",
        "config": "FDN on Regular XS WH",
        "run_id": rid, "log": log,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="IWH Benchmark Suite — 3-table matrix with warming")
    parser.add_argument("--connection", required=True,
                        help="Named connection from connections.toml")
    parser.add_argument("--phase", nargs="*", default=["A", "B"],
                        help="Phases to run (A B). Default: A B")
    parser.add_argument("--warm-wait", type=int, default=15,
                        help="Proactive cache wait per table, in minutes "
                             "(default: 15)")
    parser.add_argument("--include-baseline", action="store_true",
                        help="Also rerun the regular-WH FDN baseline "
                             "(Phase C). Off by default.")
    parser.add_argument("--mcw-size", default="XSMALL",
                        help="Warehouse size for the MCW phase (B), e.g. "
                             "XSMALL or SMALL (default: XSMALL). The IWH is "
                             "resized for phase B and reverted to XSMALL on "
                             "exit.")
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Subset of tables to run (IT FDN IB). "
                             "Default: all.")
    parser.add_argument("--mcw-levels", nargs="*", type=int, default=None,
                        help="Concurrency levels for the MCW phase (B). "
                             "Default: 500 1000. Prepend a warm-up level "
                             "(e.g. 250 500 1000) to self-warm each query.")
    parser.add_argument("--queries", nargs="*", default=None,
                        help="Query subset (e.g. point_lookup_email). "
                             "Default: point_lookup + customer360.")
    parser.add_argument("--tag-extra", nargs="*", default=None,
                        metavar="KEY=VALUE",
                        help="Extra key=value pairs merged into every "
                             "measured run's QUERY_TAG (e.g. masked=1).")
    args = parser.parse_args()

    extra_tag = {}
    if args.tag_extra:
        for pair in args.tag_extra:
            k, v = pair.split("=", 1)
            extra_tag[k] = int(v) if v.lstrip("-").isdigit() else v

    selected_tables = [t for t in TABLES
                       if not args.tables or t[0] in args.tables]

    phases = [p.upper() for p in args.phase]
    suite_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary = []

    print("=" * 70)
    print(f"IWH BENCHMARK SUITE — {suite_id}")
    print(f"Started:    {datetime.now().isoformat()}")
    print(f"Connection: {args.connection}")
    print(f"Phases:     {phases}"
          + (" + C(baseline)" if args.include_baseline else ""))
    print(f"Tables:     {[t[0] for t in selected_tables]}")
    print(f"Procs:      {PROCS}")
    print(f"Warm wait:  {args.warm_wait} min per table")
    print(f"MCW size:   {args.mcw_size}")
    print("=" * 70, flush=True)

    # Guarantee the warehouse is reverted to 1 cluster and suspended no matter
    # how a phase fails. The IWH has a 24h auto-suspend, so a crash mid-run
    # could otherwise leave a multi-cluster warehouse running (and billing).
    try:
        if "A" in phases:
            phase_single_cluster(args.connection, args.warm_wait, summary,
                                 tables=selected_tables, queries=args.queries,
                                 extra_tag=extra_tag)
        if "B" in phases:
            phase_mcw(args.connection, args.warm_wait, summary,
                      size=args.mcw_size, tables=selected_tables,
                      mcw_levels=args.mcw_levels or MCW_LEVELS,
                      queries=args.queries, extra_tag=extra_tag)
        if args.include_baseline:
            phase_regular_baseline(args.connection, summary)
    finally:
        print("\nReverting to 1 cluster, XSMALL, and suspending IWH...",
              flush=True)
        run_sql(args.connection, STD_WH,
                f"ALTER WAREHOUSE {IWH} SET MIN_CLUSTER_COUNT = 1, "
                f"MAX_CLUSTER_COUNT = 1",
                f"ALTER WAREHOUSE {IWH} SUSPEND")
        # Revert size, but only once the suspend has actually completed
        # (suspend is async; resizing while quiescing fails).
        try:
            wait_for_state(args.connection, IWH, "SUSPENDED")
            run_sql(args.connection, STD_WH,
                    f"ALTER WAREHOUSE {IWH} SET WAREHOUSE_SIZE = XSMALL")
        except RuntimeError as e:
            print(f"  WARNING: could not revert size: {e}", flush=True)

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
    if conditions:
        print(f"""
ACCOUNT_USAGE recall query (cluster count now embedded in the tag):

SELECT PARSE_JSON(query_tag):test::STRING          AS test,
       PARSE_JSON(query_tag):warehouse::STRING     AS tbl_wh,
       PARSE_JSON(query_tag):concurrency::INT      AS conc,
       PARSE_JSON(query_tag):started_clusters::INT AS clusters,
       COUNT(*)                                    AS n,
       ROUND(COUNT(*) / 60.0)                      AS qps,
       APPROX_PERCENTILE(total_elapsed_time, 0.50) AS p50,
       APPROX_PERCENTILE(total_elapsed_time, 0.90) AS p90,
       APPROX_PERCENTILE(total_elapsed_time, 0.99) AS p99,
       AVG(queued_overload_time)                   AS avg_queue_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE ({conditions})
  AND query_type = 'SELECT' AND execution_status = 'SUCCESS'
  AND start_time >= CURRENT_DATE
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3;
""")

    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary saved: {summary_file}")
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
