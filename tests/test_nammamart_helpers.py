"""
Checks the NammaMart helpers against the real dataset. Expected counts come from the
Data Issues Log in SUBMISSION.md, so the rules and the profile must agree.

Run:  python -m pytest tests/test_nammamart_helpers.py
"""
import os
import sys

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include"))

from nammamart_dq import CHECKS, freshness_result, run_checks, volume_result  # noqa: E402
from nammamart_masking import mask_customers, mask_email, mask_phone  # noqa: E402

DATA = os.path.join(ROOT, "include", "data", "nammamart")
AS_OF = pd.Timestamp("2026-10-13 09:00:00")


def load(name):
    return pd.read_csv(os.path.join(DATA, f"{name}.csv"), dtype=str, keep_default_na=False)


def validate(name, ctx):
    return run_checks(load(name), CHECKS[name], ctx, name, "test_dag", "test_run")


@pytest.fixture(scope="module")
def results():
    ctx = {"as_of": AS_OF}
    out = {}
    for master, key in [("customers", "customer_id"), ("products", "product_id"), ("stores", "store_id")]:
        out[master] = validate(master, ctx)
        ctx[master] = set(load(master)[key])
        ctx[master + "_ok"] = set(out[master][0][key])
    ctx["orders"] = set(load("orders")["order_id"])
    out["orders"] = validate("orders", ctx)
    out["payments"] = validate("payments", ctx)
    return out


def count(res, file_name, check):
    return next(r["rows_failed"] for r in res[file_name][2] if r["check_name"] == f"{file_name}.{check}")


def test_every_row_is_either_passed_or_failed(results):
    for name, (passed, failed, _) in results.items():
        assert len(passed) + len(failed) == len(load(name)), name


def test_failed_rows_always_have_a_reason(results):
    for name, (_, failed, _) in results.items():
        assert (failed["failure_reason"] != "").all(), name


def test_orders_issue_log_counts(results):
    expected = {
        "order_id_unique": 12, "customer_id_not_null": 18, "order_ts_not_null": 1,
        "order_ts_parses": 5, "order_ts_not_future": 7, "order_status_valid": 3,
        "order_status_casing": 2, "payment_mode_nonstandard": 8, "payment_mode_valid": 2, "quantity_positive": 10,
        "discount_pct_in_0_100": 3, "order_amount_matches_formula": 18,
        "delivered_has_delivery_minutes": 8, "delivery_minutes_below_120": 6,
        "product_id_in_master": 9, "store_id_in_master": 5,
        "product_master_row_quarantined": 126, "store_master_row_quarantined": 176,
    }
    for check, n in expected.items():
        assert count(results, "orders", check) == n, check


def test_payments_issue_log_counts(results):
    assert count(results, "payments", "payment_id_unique") == 4
    assert count(results, "payments", "order_id_in_orders") == 8
    assert count(results, "payments", "success_amount_positive") == 10
    assert count(results, "payments", "payment_ts_not_future") == 7


def test_masters_issue_log_counts(results):
    assert count(results, "customers", "customer_id_unique") == 5
    assert count(results, "customers", "email_format") == 12
    assert count(results, "customers", "phone_10_digits") == 7
    assert count(results, "customers", "pincode_6_digits") == 3
    assert count(results, "customers", "signup_date_not_future") == 3
    assert count(results, "products", "category_not_null") == 3
    assert count(results, "products", "product_id_unique") == 2
    assert count(results, "stores", "zone_not_null") == 1


def test_old_product_variant_loses_the_duplicate(results):
    passed = results["products"][0]
    assert not passed["product_name"].str.contains("(old)", regex=False).any()


def test_dq_results_columns_and_all_six_dimensions(results):
    rows = pd.DataFrame([r for _, _, res in results.values() for r in res])
    assert list(rows.columns) == ["run_id", "dag_id", "check_name", "dimension", "severity",
                                  "rows_checked", "rows_failed", "status", "checked_at"]
    orders_dims = set(rows[rows.check_name.str.startswith("orders.")].dimension)
    assert len(orders_dims) == 6  # the fact file covers every dimension on its own
    assert set(rows.dimension) == {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}


def test_freshness_flags_stale_data():
    assert freshness_result(load("orders"), AS_OF, "orders", "d", "r")["status"] == "WARN"
    assert freshness_result(load("orders"), pd.Timestamp("2026-09-30 23:00"), "orders", "d", "r")["status"] == "PASS"


def test_masking():
    assert mask_email("jane.doe42@example.com") == "ja***@example.com"
    assert mask_email("N/A") == "***"
    assert mask_phone("9876501234") == "98******34"
    assert mask_phone("9876501") == "98***01"
    masked = mask_customers(load("customers"))
    assert "email" not in masked and "phone" not in masked and "full_name" not in masked
    assert masked["phone_masked"].str.contains(r"\*").all()


def test_volume_drop_warns_on_a_truncated_full_file():
    assert volume_result(50, 2412, "orders.csv", "d", "r")["status"] == "WARN"
    assert volume_result(50, 2412, "orders.csv", "d", "r")["rows_failed"] == 50


def test_volume_check_ignores_normal_files_increments_and_first_runs():
    assert volume_result(2412, 2412, "orders.csv", "d", "r")["status"] == "PASS"
    assert volume_result(1900, 2412, "orders.csv", "d", "r")["status"] == "PASS"  # -21% is within tolerance
    assert volume_result(160, 2412, "orders_2026-10-01_increment.csv", "d", "r")["status"] == "PASS"
    assert volume_result(50, None, "orders.csv", "d", "r")["status"] == "PASS"  # no history yet
