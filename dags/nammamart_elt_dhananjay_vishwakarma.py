"""
dags/nammamart_elt_dhananjay_vishwakarma.py
-------------------------------------------
NammaMart ELT: Extract -> Load raw -> Transform in the warehouse with SQL.

  start_run ─► extract_orders    ─┐
               extract_payments  ─┤
               extract_customers ─┼─► load_raw ─► stg_customers ─► stg_products ─► stg_stores ─► stg_orders
               extract_products  ─┤                                                                 │
               extract_stores    ─┘                    ┌────────────────────────────────────────────┘
                                                        ▼
        stg_payments ─► mart_* (7 tasks) ─► dq_results ─► report ─► run_audit (always runs)

Layers in DuckDB (include/warehouse_dhananjay_vishwakarma.duckdb):
  raw_*   exact text copy of every CSV row, bad ones included, never cleaned
  stg_*   SQL only: cast, standardise, dedupe, mask PII, quality rules (dq_status, failure_reason)
  mart_*  SQL only: business-ready tables, PASSED rows only

Every transformation lives in include/sql/*.sql and is run by one Airflow task. Warehouse tasks run
one after another because DuckDB allows a single writer.

Incremental load: trigger with the param orders_file = orders_2026-10-01_increment.csv. The rows are
appended to raw_orders and MERGEd into stg_orders on order_id (new orders inserted, corrections updated).
"""

import csv
import sys
import time
from collections import Counter

sys.path.insert(0, "/usr/local/airflow/include")

import duckdb
import pendulum
from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

import nammamart_schema as _SCHEMA
from nammamart_alerts import notify_run, warnings_from_warehouse

NAME = "dhananjay_vishwakarma"
INCLUDE = "/usr/local/airflow/include"
DATA_DIR = f"{INCLUDE}/data/nammamart"
SQL_DIR = f"{INCLUDE}/sql"
WAREHOUSE_DB = f"{INCLUDE}/warehouse_{NAME}.duckdb"
DAG_ID = f"nammamart_elt_{NAME}"

DEFAULT_FILES, EXPECTED_COLUMNS = _SCHEMA.DEFAULT_FILES, _SCHEMA.EXPECTED_COLUMNS

default_args = {
    "owner": "fde-cohort",
    # Slowest task measured: 6.5 s (median 0.4 s). Worst case for a hung task: 3 attempts x 2 min + 2 x 30 s = 7 min,
    # which still leaves most of the 30 minutes between the 09:00 start and the 09:30 stand-up.
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=2),
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return pendulum.now("Asia/Kolkata").naive().isoformat()


def _connect() -> duckdb.DuckDBPyConnection:
    """DuckDB has one writer: wait briefly instead of failing if another task still holds the file."""
    for attempt in range(10):
        try:
            return duckdb.connect(WAREHOUSE_DB)
        except duckdb.IOException:
            if attempt == 9:
                raise
            time.sleep(3)


def _files(run: dict) -> dict:
    files = dict(DEFAULT_FILES)
    files["orders"] = run["orders_file"]
    return files


def _params(run: dict) -> dict:
    files = _files(run)
    params = {"run_id": run["run_id"], "dag_id": DAG_ID, "as_of": run["as_of"], "loaded_at": run["loaded_at"]}
    for name, file in files.items():
        params[f"{name}_file"] = file
        params[f"{name}_path"] = f"{DATA_DIR}/{file}"
    return params


def _run_sql(sql_file: str, params: dict, extra: dict | None = None) -> list:
    """Run a .sql file as one transaction. Each statement receives only the $parameters it names.
    Returns the rows of every statement that produces rows (for example MERGE ... RETURNING)."""
    values = {**params, **(extra or {})}
    con = _connect()
    returned = []
    try:
        con.execute("BEGIN")
        for path in ("elt_00_macros.sql", sql_file):
            with open(f"{SQL_DIR}/{path}") as handle:
                statements = con.extract_statements(handle.read())
            for stmt in statements:
                missing = stmt.named_parameters - values.keys()
                if missing:
                    raise KeyError(f"{path}: no value for parameter(s) {sorted(missing)}")
                con.execute(stmt, {k: values[k] for k in stmt.named_parameters})
                if con.description:
                    returned.extend(con.fetchall())
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return returned


def _query(sql: str, params: list | None = None):
    con = _connect()
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


def _staging_summary(table: str, run_id: str, run_filter: bool = False) -> None:
    where = f"WHERE _run_id = '{run_id}'" if run_filter else ""
    counts = _query(f"SELECT dq_status, count(*) AS rows FROM {table} {where} GROUP BY 1 ORDER BY 1")
    print(f"[{table}] " + ", ".join(f"{r.dq_status}={r.rows}" for r in counts.itertuples()))
    reasons = _query(f"""SELECT reason, count(*) AS rows FROM (
                             SELECT unnest(string_split(failure_reason, ';')) AS reason FROM {table} {where})
                         WHERE reason <> '' GROUP BY 1 ORDER BY 2 DESC""")
    for r in reasons.itertuples():
        print(f"[{table}]   {r.rows:>4}  {r.reason}")


@dag(
    dag_id=DAG_ID,
    # 09:00 IST, Monday to Saturday. The timezone is explicit: Airflow would otherwise read the cron in UTC.
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata"),
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    # catchup=False: after downtime Airflow starts ONE run for the latest missed 09:00 slot and skips the older
    # ones. Each run reads the current files, so replaying every missed day would only repeat the same work.
    catchup=False,
    # DuckDB allows one writer: two overlapping runs of this DAG would corrupt the audit numbers.
    max_active_runs=1,
    default_args=default_args,
    params={"as_of": "", "orders_file": "orders.csv"},
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt():

    @task
    def start_run(run_id=None, params=None):
        orders_file = (params or {}).get("orders_file") or "orders.csv"
        as_of = (params or {}).get("as_of") or _now()
        run = {"run_id": run_id, "as_of": as_of, "loaded_at": _now(), "start_time": _now(),
               "orders_file": orders_file}
        print(f"[Start] run_id={run_id} as_of={as_of} orders_file={orders_file}")
        return run

    # ── EXTRACT: five parallel tasks, one per file ───────────────────────────

    def _extract(name: str, run: dict) -> dict:
        file = _files(run)[name]
        path = f"{DATA_DIR}/{file}"
        with open(path, newline="") as handle:  # a missing file raises FileNotFoundError: the task fails loudly
            reader = csv.reader(handle)
            header = next(reader)
            rows = sum(1 for _ in reader)
        if header != EXPECTED_COLUMNS[name]:
            # A renamed or missing column must stop the run, not load wrong data. Retrying cannot fix a changed
            # file, so no retries.
            raise AirflowFailException(f"{file}: columns changed. expected {EXPECTED_COLUMNS[name]}, got {header}")
        print(f"[Extract:{name}] {rows} rows in {file}, schema OK")
        return {"name": name, "file": file, "path": path, "rows": rows}

    @task
    def extract_orders(run):
        return _extract("orders", run)

    @task
    def extract_payments(run):
        return _extract("payments", run)

    @task
    def extract_customers(run):
        return _extract("customers", run)

    @task
    def extract_products(run):
        return _extract("products", run)

    @task
    def extract_stores(run):
        return _extract("stores", run)

    # ── LOAD RAW ─────────────────────────────────────────────────────────────

    @task
    def load_raw(run, extracts):
        _run_sql("elt_01_raw_load.sql", _params(run))
        for e in extracts:
            loaded = _query(f"SELECT count(*) AS n FROM raw_{e['name']} WHERE _run_id = ?", [run["run_id"]]).n[0]
            print(f"[Load raw] raw_{e['name']}: {loaded} rows loaded from {e['file']}")
            if loaded != e["rows"]:
                raise ValueError(f"raw_{e['name']}: loaded {loaded} rows but the file has {e['rows']}")
        totals = _query("""SELECT 'raw_orders' t, count(*) n FROM raw_orders UNION ALL
                           SELECT 'raw_payments', count(*) FROM raw_payments UNION ALL
                           SELECT 'raw_customers', count(*) FROM raw_customers UNION ALL
                           SELECT 'raw_products', count(*) FROM raw_products UNION ALL
                           SELECT 'raw_stores', count(*) FROM raw_stores""")
        print("[Load raw] table totals (raw keeps every run): " + ", ".join(f"{r.t}={r.n}" for r in totals.itertuples()))

    # ── STAGING: one task per table ──────────────────────────────────────────

    @task
    def stg_customers(run):
        _run_sql("elt_10_stg_customers.sql", _params(run))
        _staging_summary("stg_customers", run["run_id"])

    @task
    def stg_products(run):
        _run_sql("elt_11_stg_products.sql", _params(run))
        _staging_summary("stg_products", run["run_id"])

    @task
    def stg_stores(run):
        _run_sql("elt_12_stg_stores.sql", _params(run))
        _staging_summary("stg_stores", run["run_id"])

    @task
    def stg_orders(run):
        merged = Counter(action for (action,) in _run_sql("elt_13_stg_orders.sql", _params(run)))
        print(f"[stg_orders] MERGE on order_id: inserted={merged['INSERT']} updated={merged['UPDATE']}")
        _staging_summary("stg_orders", run["run_id"], run_filter=True)
        total = _query("SELECT count(*) AS n, count(DISTINCT order_id) AS d FROM stg_orders").iloc[0]
        print(f"[stg_orders] table holds {total.n} rows, {total.d} distinct order_id")
        if total.n != total.d:
            raise ValueError("stg_orders has duplicate order_id after MERGE")

    @task
    def stg_payments(run):
        _run_sql("elt_14_stg_payments.sql", _params(run))
        _staging_summary("stg_payments", run["run_id"])

    # ── MARTS: one task per table ────────────────────────────────────────────

    def mart_task(task_id: str, sql_file: str):
        @task(task_id=task_id)
        def _mart(run):
            _run_sql(sql_file, _params(run))
            print(f"[{task_id}] built from {sql_file}")
        return _mart

    mart_tasks = [
        mart_task("mart_orders_enriched", "elt_20_mart_orders_enriched.sql"),
        mart_task("mart_daily_store_kpis", "elt_21_mart_daily_store_kpis.sql"),
        mart_task("mart_category_margin", "elt_22_mart_category_margin.sql"),
        mart_task("mart_delivery_sla", "elt_23_mart_delivery_sla.sql"),
        mart_task("mart_payment_reconciliation", "elt_24_mart_payment_reconciliation.sql"),
        mart_task("mart_top_customers", "elt_25_mart_top_customers.sql"),
        mart_task("mart_rejected_rows", "elt_26_mart_rejected_rows.sql"),
    ]

    # ── DATA QUALITY SCORECARD ───────────────────────────────────────────────

    @task
    def dq_results(run):
        _run_sql("elt_30_dq_catalog.sql", _params(run))
        _run_sql("elt_31_dq_results.sql", _params(run))
        summary = _query("""SELECT status, count(*) AS checks FROM dq_results
                            WHERE run_id = ? AND dag_id = ? GROUP BY 1 ORDER BY 1""", [run["run_id"], DAG_ID])
        print("[dq_results] " + ", ".join(f"{r.status}={r.checks}" for r in summary.itertuples()))

    # ── REPORT: the six business questions ───────────────────────────────────

    @task
    def report(run):
        def show(title: str, sql: str) -> None:
            print(f"[Report] {title}\n" + _query(sql).to_string(index=False))

        show("Q1 net revenue by zone", """SELECT zone, round(sum(net_revenue), 2) AS net_revenue,
                  sum(delivered_orders) AS delivered_orders FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 2 DESC""")
        show("Q2 category margin", "SELECT * FROM mart_category_margin")
        show("Q3 delivery SLA", "SELECT * FROM mart_delivery_sla")
        show("Q4 revenue by payment mode", "SELECT * FROM mart_revenue_by_payment_mode")
        show("Q5 top customers", "SELECT * FROM mart_top_customers")
        show("Q6 rejected rows this run", f"""SELECT source_file, failure_reason, rows_rejected
                  FROM mart_rejected_rows WHERE run_id = '{run['run_id']}' ORDER BY 1, 3 DESC""")
        total = _query("SELECT round(sum(net_revenue), 2) AS total FROM mart_orders_enriched").total[0]
        print(f"[Report] TOTAL net revenue = {total:,.2f}")

    # ── RUN AUDIT: always runs, even after a failure ─────────────────────────

    @task(trigger_rule="all_done")
    def run_audit(run_id=None, ti=None):
        run = ti.xcom_pull(task_ids="start_run") or {}
        states = {}
        try:
            states = ti.get_task_states(dag_id=DAG_ID, run_ids=[run_id]).get(run_id, {})
        except Exception as exc:  # the audit must not fail because it cannot read states
            print(f"[Audit] could not read task states: {exc}")
        failed = sorted(t for t, s in states.items() if s in ("failed", "upstream_failed") and t != "run_audit")
        status = "failed" if failed else "success"
        params = {"run_id": run_id, "dag_id": DAG_ID, "start_time": run.get("start_time") or _now(),
                  "end_time": _now(), "status": status}
        try:
            _run_sql("elt_40_pipeline_runs.sql", params)
        except Exception as exc:  # an early failure can leave raw/stg tables missing: record zeros
            print(f"[Audit] row counts unavailable ({exc}); recording zeros")
            con = _connect()
            try:
                con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
                    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)""")
                con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
                con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?::TIMESTAMP, ?::TIMESTAMP, 0, 0, 0, ?)",
                            [run_id, DAG_ID, params["start_time"], params["end_time"], status])
            finally:
                con.close()
        print(f"[Audit] run {run_id}: {status}" + (f" (failed tasks: {', '.join(failed)})" if failed else ""))
        audit = _query("SELECT * FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
        print(audit.to_string(index=False))
        counts = audit.iloc[0] if len(audit) else None
        root_causes = [t for t in failed if states.get(t) == "failed"] or failed  # tasks that really failed
        notify_run(DAG_ID, run_id, root_causes,
                   int(counts.rows_extracted) if counts is not None else 0,
                   int(counts.rows_loaded) if counts is not None else 0,
                   int(counts.rows_quarantined) if counts is not None else 0,
                   warnings_from_warehouse(WAREHOUSE_DB, DAG_ID, run_id),
                   skipped_tasks=len(failed) - len(root_causes))
        if failed:
            # The audit row is written first. Failing here makes the whole run red: Airflow marks a run by its
            # last task, and without this a failed extract would still end in a green run.
            raise AirflowFailException(f"run finished with failed tasks: {', '.join(failed)}")

    # ── WIRING ───────────────────────────────────────────────────────────────

    run = start_run()
    extracts = [extract_orders(run), extract_payments(run), extract_customers(run),
                extract_products(run), extract_stores(run)]
    raw = load_raw(run, extracts)

    chain = [stg_customers(run), stg_products(run), stg_stores(run), stg_orders(run), stg_payments(run)]
    chain += [t(run) for t in mart_tasks]
    chain += [dq_results(run), report(run)]

    raw >> chain[0]
    for upstream, downstream in zip(chain, chain[1:]):
        upstream >> downstream
    chain[-1] >> run_audit()


nammamart_elt()
