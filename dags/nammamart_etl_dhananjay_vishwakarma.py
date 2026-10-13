"""
dags/nammamart_etl_dhananjay_vishwakarma.py
-------------------------------------------
NammaMart ETL: Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report

  extract_customers ─► validate_customers ─┐
  extract_products  ─► validate_products  ─┼─► validate_orders ─► validate_payments ─┐
  extract_stores    ─► validate_stores    ─┘          │                              │
  extract_orders ──────────────────────────────────────┘                              │
  extract_payments ───────────────────────────────────────────────────────────────────┤
                                                                                      ▼
                    transform ─► load_warehouse ─► reconcile_payments ─► report ─► run_audit (always runs)

Only rows that pass every Critical check reach the star schema. Failed rows are written to
include/quarantine/dhananjay_vishwakarma/<file>_failed_<run_id>.csv with a failure_reason.
Warehouse writes happen in single tasks one after another: DuckDB allows one writer at a time.

Incremental load: trigger with the param orders_file = orders_2026-10-01_increment.csv. The fact table is
upserted on order_id: new orders are inserted, corrected orders replace the old row, and an order whose new
version fails validation is removed, so the newest file is always the truth.
"""

import os
import re
import sys
import time

sys.path.insert(0, "/usr/local/airflow/include")

import duckdb
import pandas as pd
import pendulum
from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

from nammamart_alerts import notify_run, warnings_from_warehouse
from nammamart_dq import (
    CHECKS, freshness_result, run_checks, volume_result,
    standardise_payment_mode, standardise_status, standardise_tier,
)
from nammamart_masking import mask_customers, mask_email, mask_phone
from nammamart_schema import DEFAULT_FILES, EXPECTED_COLUMNS

NAME = "dhananjay_vishwakarma"
INCLUDE = "/usr/local/airflow/include"
DATA_DIR = f"{INCLUDE}/data/nammamart"
WORK_DIR = f"{INCLUDE}/data/_work_{NAME}"  # unmasked intermediates stay under include/data
QUARANTINE_DIR = f"{INCLUDE}/quarantine/{NAME}"
WAREHOUSE_DB = f"{INCLUDE}/warehouse_{NAME}.duckdb"
DAG_ID = f"nammamart_etl_{NAME}"

FILES = DEFAULT_FILES

default_args = {
    "owner": "fde-cohort",
    # Slowest task measured: 6.5 s (median 0.4 s). Worst case for a hung task: 3 attempts x 2 min + 2 x 30 s = 7 min,
    # which still leaves most of the 30 minutes between the 09:00 start and the 09:30 stand-up.
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=2),
}

DDL = {
    "etl_dim_customer": """
        customer_id VARCHAR, email_masked VARCHAR, phone_masked VARCHAR, city VARCHAR, area VARCHAR,
        pincode VARCHAR, signup_date DATE, loyalty_tier VARCHAR, is_active VARCHAR""",
    "etl_dim_product": """
        product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
        mrp DOUBLE, cost_price DOUBLE, unit VARCHAR, is_perishable VARCHAR, launch_date DATE""",
    "etl_dim_store": """
        store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR,
        open_date DATE, manager_name VARCHAR, capacity_orders_per_day INTEGER""",
    "etl_fact_orders": """
        order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, store_id VARCHAR,
        product_id VARCHAR, quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
        payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
        net_revenue DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN, warning_reason VARCHAR,
        _source_file VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP""",
    "etl_order_ids_seen": "order_id VARCHAR",
    "etl_volume_history": "run_id VARCHAR, file_name VARCHAR, rows_in_file BIGINT, recorded_at TIMESTAMP",
    "pipeline_runs": """
        run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
        rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR""",
    "dq_results": """
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
        rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP""",
}


def _connect_writer() -> duckdb.DuckDBPyConnection:
    """DuckDB has one writer: wait briefly instead of failing if another task still holds the file."""
    for attempt in range(10):
        try:
            return duckdb.connect(WAREHOUSE_DB)
        except duckdb.IOException:
            if attempt == 9:
                raise
            time.sleep(3)


def _safe(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _read(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _work(run_id: str, name: str) -> str:
    path = f"{WORK_DIR}/{_safe(run_id)}"
    os.makedirs(path, exist_ok=True)
    return f"{path}/{name}"


def _as_of(params: dict) -> pd.Timestamp:
    """Run time in naive IST. The `as_of` param lets experiments replay a past date."""
    given = (params or {}).get("as_of") or ""
    return pd.Timestamp(given) if given else pendulum.now("Asia/Kolkata").naive()


def _seen_order_ids() -> set:
    """Every order_id any earlier run has read, valid or not. A payment may belong to an order that
    arrived in an earlier file, so payments are checked against this history plus the current file."""
    if not os.path.exists(WAREHOUSE_DB):
        return set()
    con = duckdb.connect(WAREHOUSE_DB, read_only=True)
    try:
        if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'etl_order_ids_seen'").fetchone()[0]:
            return set()
        return {row[0] for row in con.execute("SELECT order_id FROM etl_order_ids_seen").fetchall()}
    finally:
        con.close()


def _previous_max_volume() -> int | None:
    """Row count of the largest earlier full orders file (increment files excluded), or None on a first run."""
    if not os.path.exists(WAREHOUSE_DB):
        return None
    con = duckdb.connect(WAREHOUSE_DB, read_only=True)
    try:
        if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'etl_volume_history'").fetchone()[0]:
            return None
        return con.execute("SELECT max(rows_in_file) FROM etl_volume_history WHERE lower(file_name) NOT LIKE '%increment%'").fetchone()[0]
    finally:
        con.close()


def _validate(name: str, extract: dict, ctx: dict, run_id: str) -> dict:
    """Run the quality checks for one file, write passed rows and the quarantine file."""
    df = _read(extract["path"])
    passed, failed, results = run_checks(df, CHECKS[name], ctx, name, DAG_ID, run_id)
    if name == "orders":
        results.append(freshness_result(df, ctx["as_of"], name, DAG_ID, run_id))
        results.append(volume_result(len(df), ctx.get("previous_volume"), extract["file_name"], DAG_ID, run_id))

    passed_path = _work(run_id, f"{name}_passed.csv")
    passed.to_csv(passed_path, index=False)

    quarantine_path = None
    if len(failed):
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_path = f"{QUARANTINE_DIR}/{name}_failed_{_safe(run_id)}.csv"
        out = failed
        if name == "customers":  # never write raw personal data outside the warehouse
            out = failed.assign(email=failed["email"].map(mask_email), phone=failed["phone"].map(mask_phone))
            out = out.drop(columns=["full_name"])
        out.to_csv(quarantine_path, index=False)

    for r in results:
        r["checked_at"] = r["checked_at"].isoformat()
    print(f"[Validate:{name}] in={len(df)} passed={len(passed)} quarantined={len(failed)}")
    for reason, n in failed["failure_reason"].str.split(";").explode().value_counts().items():
        print(f"[Validate:{name}]   {n:>4}  {reason}")
    return {
        "file": name,
        "source_file": extract["file_name"],
        "raw_path": extract["path"],
        "rows_in": len(df),
        "rows_passed": len(passed),
        "rows_quarantined": len(failed),
        "passed_path": passed_path,
        "quarantine_path": quarantine_path,
        "dq_results": results,
    }


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
    tags=["fde", "nammamart", "etl"],
)
def nammamart_etl():

    # ── EXTRACT: five parallel tasks, one per file ───────────────────────────

    def _extract(name: str, run_id: str, file_name: str) -> dict:
        source = f"{DATA_DIR}/{file_name}"
        df = _read(source)  # a missing file raises FileNotFoundError and fails the task loudly
        if list(df.columns) != EXPECTED_COLUMNS[name]:
            # A renamed column must stop the run, not load wrong data. Retrying cannot fix a changed file, so no retries.
            raise AirflowFailException(
                f"{file_name}: columns changed. expected {EXPECTED_COLUMNS[name]}, got {list(df.columns)}")
        path = _work(run_id, f"{name}_raw.csv")
        df.to_csv(path, index=False)
        print(f"[Extract:{name}] {len(df)} rows from {file_name}, schema OK")
        return {"file": name, "file_name": file_name, "path": path, "rows": len(df)}

    @task
    def extract_orders(run_id=None, params=None):
        # the next-day increment is loaded by triggering with orders_file = orders_2026-10-01_increment.csv
        return _extract("orders", run_id, (params or {}).get("orders_file") or FILES["orders"])

    @task
    def extract_payments(run_id=None):
        return _extract("payments", run_id, FILES["payments"])

    @task
    def extract_customers(run_id=None):
        return _extract("customers", run_id, FILES["customers"])

    @task
    def extract_products(run_id=None):
        return _extract("products", run_id, FILES["products"])

    @task
    def extract_stores(run_id=None):
        return _extract("stores", run_id, FILES["stores"])

    # ── VALIDATE + QUARANTINE: masters first, because orders are checked against them ──

    @task
    def validate_customers(extract, run_id=None, params=None):
        return _validate("customers", extract, {"as_of": _as_of(params)}, run_id)

    @task
    def validate_products(extract, run_id=None, params=None):
        return _validate("products", extract, {"as_of": _as_of(params)}, run_id)

    @task
    def validate_stores(extract, run_id=None, params=None):
        return _validate("stores", extract, {"as_of": _as_of(params)}, run_id)

    @task
    def validate_orders(extract, customers, products, stores, run_id=None, params=None):
        ctx = {"as_of": _as_of(params), "previous_volume": _previous_max_volume()}
        for key, res, col in [("customers", customers, "customer_id"), ("products", products, "product_id"),
                              ("stores", stores, "store_id")]:
            ctx[key] = set(_read(res["raw_path"])[col])  # ids that exist in the raw master
            ctx[key + "_ok"] = set(_read(res["passed_path"])[col])  # ids whose master row passed
        return _validate("orders", extract, ctx, run_id)

    @task
    def validate_payments(extract, orders, run_id=None, params=None):
        ctx = {"as_of": _as_of(params),
               "orders": set(_read(orders["raw_path"])["order_id"]) | _seen_order_ids()}
        return _validate("payments", extract, ctx, run_id)

    # ── TRANSFORM ────────────────────────────────────────────────────────────

    @task
    def transform(customers, products, stores, orders, run_id=None):
        cust = _read(customers["passed_path"]).drop(columns="warning_reason")
        cust["loyalty_tier"] = standardise_tier(cust["loyalty_tier"])
        cust = cust.drop_duplicates("customer_id")
        cust["signup_date"] = pd.to_datetime(cust["signup_date"]).dt.date
        dim_customer = mask_customers(cust)[
            ["customer_id", "email_masked", "phone_masked", "city", "area", "pincode",
             "signup_date", "loyalty_tier", "is_active"]]

        prod = _read(products["passed_path"]).drop(columns="warning_reason").drop_duplicates("product_id")
        for col in ("mrp", "cost_price"):
            prod[col] = pd.to_numeric(prod[col])
        prod["launch_date"] = pd.to_datetime(prod["launch_date"]).dt.date
        dim_product = prod.drop(columns=[], errors="ignore")

        store = _read(stores["passed_path"]).drop(columns="warning_reason").drop_duplicates("store_id")
        store["open_date"] = pd.to_datetime(store["open_date"]).dt.date
        store["capacity_orders_per_day"] = pd.to_numeric(store["capacity_orders_per_day"]).astype(int)
        dim_store = store

        o = _read(orders["passed_path"])
        o["order_status"] = standardise_status(o["order_status"])
        o["payment_mode"] = standardise_payment_mode(o["payment_mode"])
        o["order_ts"] = pd.to_datetime(o["order_ts"].str.strip(), format="%Y-%m-%d %H:%M:%S")
        o["order_date"] = o["order_ts"].dt.date
        o["quantity"] = pd.to_numeric(o["quantity"]).astype(int)
        for col in ("unit_price", "discount_pct", "order_amount", "delivery_minutes"):
            o[col] = pd.to_numeric(o[col], errors="coerce")  # blank delivery_minutes -> NaN (non-delivered)
        o = o.drop_duplicates("order_id")

        before = len(o)
        fact = (o.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="inner")
                 .merge(dim_store[["store_id"]], on="store_id", how="inner")
                 .merge(dim_customer[["customer_id"]], on="customer_id", how="inner"))
        if len(fact) != before:  # validation guarantees every key resolves; anything else is a bug
            raise ValueError(f"join changed the order count: {before} -> {len(fact)}")

        delivered = fact["order_status"].eq("DELIVERED")
        fact["net_revenue"] = fact["order_amount"].where(delivered, 0.0).round(2)
        fact["gross_margin"] = (fact["net_revenue"] - fact["quantity"] * fact["cost_price"]).where(delivered, 0.0).round(2)
        fact["within_15_min"] = fact["delivery_minutes"].le(15).astype("boolean").where(delivered)
        fact["_source_file"] = orders["source_file"]
        fact["_run_id"] = run_id
        fact["_loaded_at"] = pendulum.now("Asia/Kolkata").naive()
        fact = fact[[c.split()[0] for c in DDL["etl_fact_orders"].replace("\n", " ").split(",")]]

        outputs = {"etl_dim_customer": dim_customer, "etl_dim_product": dim_product,
                   "etl_dim_store": dim_store, "etl_fact_orders": fact}
        paths = {}
        for table, frame in outputs.items():
            paths[table] = _work(run_id, f"{table}.pkl")
            frame.to_pickle(paths[table])
        print(f"[Transform] fact={len(fact)} customers={len(dim_customer)} products={len(dim_product)} "
              f"stores={len(dim_store)}  delivered net_revenue={fact['net_revenue'].sum():,.2f}")
        return paths

    # ── LOAD: idempotent ─────────────────────────────────────────────────────

    @task
    def load_warehouse(paths, validations, run_id=None):
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute("BEGIN")
            for table, cols in DDL.items():
                con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")

            for table in ("etl_dim_customer", "etl_dim_product", "etl_dim_store"):
                frame = pd.read_pickle(paths[table])
                con.execute(f"DELETE FROM {table}")  # dimensions are small: full refresh
                con.register("stage", frame)
                con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM stage")
                con.unregister("stage")

            # Upsert on order_id. The newest file is the truth for every order it contains, so each incoming
            # id is deleted first, including ids whose new version FAILED validation: the old version must not
            # survive in the fact table. Then the passed rows are inserted.
            fact = pd.read_pickle(paths["etl_fact_orders"])
            orders_run = next(v for v in validations if v["file"] == "orders")
            incoming = pd.DataFrame({"order_id": _read(orders_run["raw_path"])["order_id"].unique()})
            con.register("incoming", incoming)
            con.register("stage", fact)
            existing = {r[0] for r in con.execute(
                "SELECT order_id FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM incoming)").fetchall()}
            inserted = int((~fact["order_id"].isin(existing)).sum())
            updated = int(fact["order_id"].isin(existing).sum())
            removed = len(existing) - updated  # existed before, rejected now
            con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM incoming)")
            con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM stage")
            con.execute("""INSERT INTO etl_order_ids_seen
                           SELECT order_id FROM incoming WHERE order_id NOT IN (SELECT order_id FROM etl_order_ids_seen)""")
            con.unregister("stage")
            con.unregister("incoming")
            con.execute("DELETE FROM etl_volume_history WHERE run_id = ?", [run_id])
            con.execute("INSERT INTO etl_volume_history VALUES (?, ?, ?, ?)",
                        [run_id, orders_run["source_file"], orders_run["rows_in"], pendulum.now("Asia/Kolkata").naive()])
            print(f"[Load] etl_fact_orders upsert on order_id: inserted={inserted} updated={updated} "
                  f"removed_because_now_rejected={removed}")

            dq = pd.DataFrame([r for v in validations for r in v["dq_results"]])
            dq["checked_at"] = pd.to_datetime(dq["checked_at"])
            con.register("stage", dq)
            con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute("INSERT INTO dq_results BY NAME SELECT * FROM stage")
            con.unregister("stage")

            rows, distinct = con.execute("SELECT count(*), count(DISTINCT order_id) FROM etl_fact_orders").fetchone()
            if rows != distinct:
                raise ValueError(f"etl_fact_orders has duplicate order_id: {rows} rows, {distinct} distinct")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        print(f"[Load] etl_fact_orders now holds {rows} rows")
        return {"fact_rows": rows, "inserted": inserted, "updated": updated, "removed": removed}

    # ── RECONCILE ────────────────────────────────────────────────────────────

    @task
    def reconcile_payments(load, payments, run_id=None):
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            pay = _read(payments["passed_path"]).drop(columns="warning_reason")
            pay["payment_mode"] = standardise_payment_mode(pay["payment_mode"])
            pay["amount_paid"] = pd.to_numeric(pay["amount_paid"])
            orders = con.execute(
                "SELECT order_id, order_status, order_amount FROM etl_fact_orders").df()
            merged = orders.merge(pay[["payment_id", "order_id", "payment_status", "amount_paid"]],
                                  on="order_id", how="outer", indicator=True)
            merged["difference"] = (merged["amount_paid"] - merged["order_amount"]).round(2)

            expected = {"DELIVERED": "SUCCESS", "RETURNED": "REFUNDED"}
            reason = pd.Series(None, index=merged.index, dtype="object")
            has_order, has_pay = merged["_merge"] != "right_only", merged["_merge"] != "left_only"
            reason[~has_pay & merged["order_status"].ne("CANCELLED")] = "missing_payment"
            reason[~has_order] = "payment_without_clean_order"
            wrong_status = has_order & has_pay & merged["payment_status"].ne(merged["order_status"].map(expected))
            reason[wrong_status] = ("payment_status_" + merged["payment_status"].str.lower()
                                    + "_but_order_" + merged["order_status"].str.lower())
            amount_off = (has_order & has_pay & merged["payment_status"].eq("SUCCESS")
                          & (merged["difference"].abs() > 0.0101))
            reason[amount_off & reason.isna()] = "amount_mismatch"

            recon = merged[reason.notna()].assign(reason=reason[reason.notna()], _run_id=run_id)
            recon = recon[["order_id", "payment_id", "order_status", "payment_status", "order_amount",
                           "amount_paid", "difference", "reason", "_run_id"]]
            con.register("recon", recon)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
            con.unregister("recon")
        finally:
            con.close()
        print(f"[Reconcile] {len(recon)} mismatches")
        print(recon["reason"].value_counts().to_string())
        unreconciled = recon.loc[recon["reason"].eq("amount_mismatch"), "difference"].abs().sum()
        print(f"[Reconcile] total amount_mismatch value = {unreconciled:,.2f}")
        return {"mismatches": len(recon)}

    # ── REPORT ───────────────────────────────────────────────────────────────

    @task
    def report(recon):
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT f.order_date, s.zone, s.store_id, s.store_name,
                       count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
                       round(sum(f.net_revenue), 2) AS net_revenue
                FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
                GROUP BY ALL ORDER BY 1, 2, 3""")
            con.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT p.category,
                       round(sum(f.net_revenue), 2) AS revenue,
                       round(sum(f.gross_margin), 2) AS gross_margin,
                       round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
                FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
                GROUP BY ALL ORDER BY revenue DESC""")
            zone = con.execute("""SELECT zone, round(sum(net_revenue), 2) AS net_revenue, sum(delivered_orders) AS delivered_orders
                                  FROM etl_daily_zone_revenue GROUP BY zone ORDER BY 2 DESC""").df()
            margin = con.execute("SELECT * FROM etl_category_margin").df()
        finally:
            con.close()
        print("[Report] net revenue by zone\n" + zone.to_string(index=False))
        print("[Report] category margin\n" + margin.to_string(index=False))
        print(f"[Report] TOTAL net revenue = {margin['revenue'].sum():,.2f}")

    # ── RUN AUDIT: always runs, even after a failure ─────────────────────────

    @task(trigger_rule="all_done")
    def run_audit(run_id=None, ti=None):
        context = get_current_context()
        start = context["dag_run"].start_date
        start_time = pendulum.instance(start).in_timezone("Asia/Kolkata").naive() if start else pendulum.now("Asia/Kolkata").naive()
        states = {}
        try:
            states = ti.get_task_states(dag_id=DAG_ID, run_ids=[run_id]).get(run_id, {})
        except Exception as exc:  # the audit must not fail because it cannot read task states
            print(f"[Audit] could not read task states: {exc}")
        failed = sorted(t for t, st in states.items() if st in ("failed", "upstream_failed") and t != "run_audit")

        extracts = [x for x in ti.xcom_pull(task_ids=[f"extract_{n}" for n in FILES]) or [] if x]
        validations = [v for v in ti.xcom_pull(
            task_ids=[f"validate_{n}" for n in ("customers", "products", "stores", "orders", "payments")]) or [] if v]
        loaded = states.get("load_warehouse") == "success"
        counts = (
            sum(x["rows"] for x in extracts),
            sum(v["rows_passed"] for v in validations) if loaded else 0,  # rows that reached the warehouse
            sum(v["rows_quarantined"] for v in validations),
        )
        status = "failed" if failed else "success"

        con = _connect_writer()
        try:
            con.execute(f"CREATE TABLE IF NOT EXISTS pipeline_runs ({DDL['pipeline_runs']})")
            con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [run_id, DAG_ID, start_time, pendulum.now("Asia/Kolkata").naive(), *counts, status])
            audit = con.execute("SELECT * FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID]).df()
        finally:
            con.close()
        print(f"[Audit] run {run_id}: {status}" + (f" (failed tasks: {', '.join(failed)})" if failed else ""))
        print(audit.to_string(index=False))
        root_causes = [t for t in failed if states.get(t) == "failed"] or failed  # tasks that really failed
        notify_run(DAG_ID, run_id, root_causes, *counts, warnings_from_warehouse(WAREHOUSE_DB, DAG_ID, run_id),
                   skipped_tasks=len(failed) - len(root_causes))
        if failed:
            # The audit row is written first. Failing here makes the whole run red: Airflow marks a run by its
            # last task, and without this a failed extract would still end in a green run.
            raise AirflowFailException(f"run finished with failed tasks: {', '.join(failed)}")

    # ── WIRING ───────────────────────────────────────────────────────────────

    e_orders, e_payments = extract_orders(), extract_payments()
    e_customers, e_products, e_stores = extract_customers(), extract_products(), extract_stores()

    v_customers = validate_customers(e_customers)
    v_products = validate_products(e_products)
    v_stores = validate_stores(e_stores)
    v_orders = validate_orders(e_orders, v_customers, v_products, v_stores)
    v_payments = validate_payments(e_payments, v_orders)

    paths = transform(v_customers, v_products, v_stores, v_orders)
    loaded = load_warehouse(paths, [v_customers, v_products, v_stores, v_orders, v_payments])
    recon = reconcile_payments(loaded, v_payments)
    report(recon) >> run_audit()


nammamart_etl()
