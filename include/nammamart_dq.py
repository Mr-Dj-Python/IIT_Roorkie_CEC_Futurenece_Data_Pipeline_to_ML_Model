"""
include/nammamart_dq.py
-----------------------
Data quality rules for the NammaMart ETL pipeline (pandas).

Every check belongs to one of six dimensions and has a severity:
    Critical -> the row is quarantined with a failure_reason
    Warning  -> the row loads, the issue is logged in warning_reason and dq_results

Usage:
    passed, failed, results = run_checks(df, CHECKS["orders"], ctx, "orders", dag_id, run_id)

`ctx` carries reference data for cross-file checks:
    as_of      pd.Timestamp  run time (naive IST); nothing may be dated after it
    customers, products, stores   sets of ids in the raw master files (existence)
    customers_ok, products_ok, stores_ok   sets of ids whose master row passed validation
    orders        set of ids in the raw orders file

Read CSVs with dtype=str, keep_default_na=False so that 'N/A' and '' are visible to the checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

CRITICAL, WARNING = "Critical", "Warning"
COMPLETENESS, UNIQUENESS, VALIDITY = "Completeness", "Uniqueness", "Validity"
ACCURACY, CONSISTENCY, TIMELINESS = "Accuracy", "Consistency", "Timeliness"

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
VALID_ZONES = {"North", "South", "East", "West"}
VALID_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "wallet": "Wallet",
}  # anything else (e.g. 'Cashh') is ambiguous and is rejected, not guessed

AMOUNT_TOLERANCE = 0.0101  # two-decimal rounding plus float noise


# ── parsing helpers ──────────────────────────────────────────────────────────

def _blank(s: pd.Series) -> pd.Series:
    return s.isna() | s.astype(str).str.strip().eq("")


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.astype(str).str.strip(), format="%Y-%m-%d %H:%M:%S", errors="coerce")


def _date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.astype(str).str.strip(), format="%Y-%m-%d", errors="coerce")


def _status(d: pd.DataFrame) -> pd.Series:
    return d["order_status"].astype(str).str.strip().str.upper()


def standardise_payment_mode(s: pd.Series) -> pd.Series:
    """Trim, lowercase and map to the canonical value. Unmappable values become NaN."""
    return s.astype(str).str.strip().str.lower().map(PAYMENT_MODE_MAP)


def standardise_status(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()


def standardise_tier(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.title()


def _dup_rejects(df: pd.DataFrame, key: str, prefer: Callable[[pd.DataFrame], pd.Series] | None = None) -> pd.Series:
    """True for every row that is a repeat of an earlier key. The best row (lowest `prefer`,
    then file order) is kept, so exactly one row per key survives."""
    score = prefer(df) if prefer else pd.Series(0, index=df.index)
    ordered = df.assign(_p=score).sort_values("_p", kind="stable")
    repeat = ordered.duplicated(key, keep="first") & ~_blank(ordered[key])
    return repeat.reindex(df.index)


def _prefer_not_old(df: pd.DataFrame) -> pd.Series:
    return df["product_name"].str.contains(r"\(old\)", case=False, regex=True).astype(int)


# ── check definition ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Check:
    name: str
    dimension: str
    severity: str
    fails: Callable[[pd.DataFrame, dict], pd.Series]  # True where the row breaks the rule


ORDER_CHECKS = [
    Check("customer_id_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["customer_id"])),
    Check("order_ts_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["order_ts"])),
    Check("delivered_has_delivery_minutes", COMPLETENESS, CRITICAL,
          lambda d, c: _status(d).eq("DELIVERED") & _blank(d["delivery_minutes"])),
    Check("order_id_unique", UNIQUENESS, CRITICAL, lambda d, c: _dup_rejects(d, "order_id")),
    Check("order_status_valid", VALIDITY, CRITICAL, lambda d, c: ~_status(d).isin(VALID_STATUS)),
    Check("order_status_casing", VALIDITY, WARNING,
          lambda d, c: _status(d).isin(VALID_STATUS) & d["order_status"].ne(_status(d))),
    Check("payment_mode_valid", VALIDITY, CRITICAL, lambda d, c: standardise_payment_mode(d["payment_mode"]).isna()),
    Check("payment_mode_nonstandard", VALIDITY, WARNING,
          lambda d, c: standardise_payment_mode(d["payment_mode"]).notna()
          & d["payment_mode"].ne(standardise_payment_mode(d["payment_mode"]))),
    Check("order_ts_parses", VALIDITY, CRITICAL, lambda d, c: ~_blank(d["order_ts"]) & _ts(d["order_ts"]).isna()),
    Check("numeric_columns_parse", VALIDITY, CRITICAL,
          lambda d, c: _num(d["quantity"]).isna() | _num(d["unit_price"]).isna()
          | _num(d["discount_pct"]).isna() | _num(d["order_amount"]).isna()
          | (~_blank(d["delivery_minutes"]) & _num(d["delivery_minutes"]).isna())),
    Check("discount_pct_in_0_100", VALIDITY, CRITICAL,
          lambda d, c: (_num(d["discount_pct"]) < 0) | (_num(d["discount_pct"]) > 100)),
    Check("quantity_positive", ACCURACY, CRITICAL, lambda d, c: _num(d["quantity"]) <= 0),
    Check("unit_price_positive", ACCURACY, CRITICAL, lambda d, c: _num(d["unit_price"]) <= 0),
    Check("order_amount_matches_formula", ACCURACY, CRITICAL,
          lambda d, c: (_num(d["order_amount"])
                        - (_num(d["quantity"]) * _num(d["unit_price"]) * (1 - _num(d["discount_pct"]) / 100)).round(2)
                        ).abs() > AMOUNT_TOLERANCE),
    Check("delivery_minutes_below_120", ACCURACY, CRITICAL, lambda d, c: _num(d["delivery_minutes"]) >= 120),
    Check("delivery_minutes_in_8_35", ACCURACY, WARNING,
          lambda d, c: (_num(d["delivery_minutes"]) < 8) | (_num(d["delivery_minutes"]) > 35)),
    Check("customer_id_in_master", CONSISTENCY, CRITICAL,
          lambda d, c: ~_blank(d["customer_id"]) & ~d["customer_id"].isin(c["customers"])),
    Check("product_id_in_master", CONSISTENCY, CRITICAL, lambda d, c: ~d["product_id"].isin(c["products"])),
    Check("store_id_in_master", CONSISTENCY, CRITICAL, lambda d, c: ~d["store_id"].isin(c["stores"])),
    # the id exists in the raw master but that master row was quarantined, so the order cannot be joined
    Check("customer_master_row_quarantined", CONSISTENCY, CRITICAL,
          lambda d, c: d["customer_id"].isin(c["customers"]) & ~d["customer_id"].isin(c["customers_ok"])),
    Check("product_master_row_quarantined", CONSISTENCY, CRITICAL,
          lambda d, c: d["product_id"].isin(c["products"]) & ~d["product_id"].isin(c["products_ok"])),
    Check("store_master_row_quarantined", CONSISTENCY, CRITICAL,
          lambda d, c: d["store_id"].isin(c["stores"]) & ~d["store_id"].isin(c["stores_ok"])),
    Check("order_ts_not_future", TIMELINESS, CRITICAL, lambda d, c: _ts(d["order_ts"]) > c["as_of"]),
]

PAYMENT_CHECKS = [
    Check("payment_id_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["payment_id"])),
    Check("payment_id_unique", UNIQUENESS, CRITICAL, lambda d, c: _dup_rejects(d, "payment_id")),
    Check("payment_status_valid", VALIDITY, CRITICAL,
          lambda d, c: ~d["payment_status"].astype(str).str.strip().str.upper().isin(VALID_PAYMENT_STATUS)),
    Check("payment_mode_valid", VALIDITY, CRITICAL, lambda d, c: standardise_payment_mode(d["payment_mode"]).isna()),
    Check("payment_mode_nonstandard", VALIDITY, WARNING,
          lambda d, c: standardise_payment_mode(d["payment_mode"]).notna()
          & d["payment_mode"].ne(standardise_payment_mode(d["payment_mode"]))),
    Check("payment_ts_parses", VALIDITY, CRITICAL, lambda d, c: _ts(d["payment_ts"]).isna()),
    Check("amount_paid_numeric", VALIDITY, CRITICAL, lambda d, c: _num(d["amount_paid"]).isna()),
    Check("success_amount_positive", ACCURACY, CRITICAL,
          lambda d, c: d["payment_status"].eq("SUCCESS") & (_num(d["amount_paid"]) <= 0)),
    Check("order_id_in_orders", CONSISTENCY, CRITICAL, lambda d, c: ~d["order_id"].isin(c["orders"])),
    Check("payment_ts_not_future", TIMELINESS, CRITICAL, lambda d, c: _ts(d["payment_ts"]) > c["as_of"]),
]

CUSTOMER_CHECKS = [
    Check("customer_id_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["customer_id"])),
    Check("city_not_null", COMPLETENESS, WARNING, lambda d, c: _blank(d["city"])),
    Check("customer_id_unique", UNIQUENESS, CRITICAL, lambda d, c: _dup_rejects(d, "customer_id")),
    Check("email_format", VALIDITY, WARNING,
          lambda d, c: ~d["email"].astype(str).str.strip().str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")),
    Check("phone_10_digits", VALIDITY, WARNING,
          lambda d, c: ~d["phone"].astype(str).str.strip().str.match(r"^\d{10}$")),
    Check("pincode_6_digits", VALIDITY, WARNING,
          lambda d, c: ~d["pincode"].astype(str).str.strip().str.match(r"^\d{6}$")),
    Check("signup_date_parses", VALIDITY, CRITICAL, lambda d, c: _date(d["signup_date"]).isna()),
    Check("loyalty_tier_valid", VALIDITY, CRITICAL, lambda d, c: ~standardise_tier(d["loyalty_tier"]).isin(VALID_TIERS)),
    Check("loyalty_tier_casing", CONSISTENCY, WARNING,
          lambda d, c: standardise_tier(d["loyalty_tier"]).isin(VALID_TIERS)
          & d["loyalty_tier"].ne(standardise_tier(d["loyalty_tier"]))),
    Check("signup_date_not_future", TIMELINESS, CRITICAL, lambda d, c: _date(d["signup_date"]) > c["as_of"]),
]

PRODUCT_CHECKS = [
    Check("category_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["category"])),
    Check("product_id_unique", UNIQUENESS, CRITICAL, lambda d, c: _dup_rejects(d, "product_id", _prefer_not_old)),
    Check("price_columns_numeric", VALIDITY, CRITICAL,
          lambda d, c: _num(d["mrp"]).isna() | _num(d["cost_price"]).isna()),
    Check("launch_date_parses", VALIDITY, CRITICAL, lambda d, c: _date(d["launch_date"]).isna()),
    Check("mrp_positive", ACCURACY, CRITICAL, lambda d, c: _num(d["mrp"]) <= 0),
    Check("cost_price_positive", ACCURACY, CRITICAL, lambda d, c: _num(d["cost_price"]) <= 0),
    Check("cost_price_not_above_mrp", ACCURACY, CRITICAL, lambda d, c: _num(d["cost_price"]) > _num(d["mrp"])),
    Check("launch_date_not_future", TIMELINESS, CRITICAL, lambda d, c: _date(d["launch_date"]) > c["as_of"]),
]

STORE_CHECKS = [
    Check("zone_not_null", COMPLETENESS, CRITICAL, lambda d, c: _blank(d["zone"])),
    Check("store_id_unique", UNIQUENESS, CRITICAL, lambda d, c: _dup_rejects(d, "store_id")),
    Check("zone_valid", VALIDITY, CRITICAL, lambda d, c: ~_blank(d["zone"]) & ~d["zone"].isin(VALID_ZONES)),
    Check("pincode_6_digits", VALIDITY, WARNING,
          lambda d, c: ~d["pincode"].astype(str).str.strip().str.match(r"^\d{6}$")),
    Check("capacity_positive", ACCURACY, WARNING, lambda d, c: _num(d["capacity_orders_per_day"]) <= 0),
    Check("open_date_not_future", TIMELINESS, CRITICAL, lambda d, c: _date(d["open_date"]) > c["as_of"]),
]

CHECKS = {
    "orders": ORDER_CHECKS,
    "payments": PAYMENT_CHECKS,
    "customers": CUSTOMER_CHECKS,
    "products": PRODUCT_CHECKS,
    "stores": STORE_CHECKS,
}


# ── runner ───────────────────────────────────────────────────────────────────

def run_checks(df: pd.DataFrame, checks: list[Check], ctx: dict, file_name: str,
               dag_id: str, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """Apply every check to df.

    Returns (passed, failed, dq_results).
      passed   rows with no Critical failure, plus warning_reason (empty when clean)
      failed   rows with a Critical failure, plus failure_reason (all Critical checks they broke)
      dq_results  one dict per check with the dq_results table columns
    """
    checked_at = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    masks = {c.name: c.fails(df, ctx).fillna(False).astype(bool) for c in checks}

    results = [
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "check_name": f"{file_name}.{c.name}",
            "dimension": c.dimension,
            "severity": c.severity,
            "rows_checked": len(df),
            "rows_failed": int(masks[c.name].sum()),
            "status": "PASS" if not masks[c.name].any() else ("FAIL" if c.severity == CRITICAL else "WARN"),
            "checked_at": checked_at,
        }
        for c in checks
    ]

    def reasons(names: list[str]) -> pd.Series:
        if not names or df.empty:
            return pd.Series("", index=df.index)
        m = pd.DataFrame({n: masks[n] for n in names})
        return m.apply(lambda row: ";".join(row.index[row.values]), axis=1)

    critical = [c.name for c in checks if c.severity == CRITICAL]
    warning = [c.name for c in checks if c.severity == WARNING]
    crit_reason, warn_reason = reasons(critical), reasons(warning)
    is_failed = crit_reason.ne("")

    failed = df[is_failed].assign(failure_reason=crit_reason[is_failed])
    passed = df[~is_failed].assign(warning_reason=warn_reason[~is_failed])
    return passed, failed, results


def freshness_result(df: pd.DataFrame, as_of: pd.Timestamp, file_name: str, dag_id: str, run_id: str,
                     max_age_days: float = 1.0) -> dict:
    """Dataset-level Timeliness (Warning): latest order_ts must be within max_age_days of run time.
    Future-dated rows are ignored here: they are quarantined and must not mask a stale feed."""
    ts = _ts(df["order_ts"])
    latest = ts[ts <= as_of].max()
    stale = pd.isna(latest) or (as_of - latest) > pd.Timedelta(days=max_age_days)
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": f"{file_name}.latest_order_not_stale",
        "dimension": TIMELINESS,
        "severity": WARNING,
        "rows_checked": len(df),
        "rows_failed": len(df) if stale else 0,
        "status": "WARN" if stale else "PASS",
        "checked_at": pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None),
    }


def volume_result(rows: int, baseline: int | None, file_name: str, dag_id: str, run_id: str,
                  max_drop: float = 0.9) -> dict:
    """Dataset-level Completeness (Warning): a full orders file with more than `max_drop` fewer rows than the
    largest earlier full file is suspicious (truncated export, partial extract). Increment files are small by
    design and have no baseline, so they pass. The baseline is the MAXIMUM of earlier runs, so one anomalous
    run cannot lower the bar for the next."""
    dropped = (baseline is not None and "increment" not in file_name.lower() and rows < (1 - max_drop) * baseline)
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": "orders.volume_drop_vs_history",
        "dimension": COMPLETENESS,
        "severity": WARNING,
        "rows_checked": rows,
        "rows_failed": rows if dropped else 0,
        "status": "WARN" if dropped else "PASS",
        "checked_at": pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None),
    }
