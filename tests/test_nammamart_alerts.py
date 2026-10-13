"""
Tests for include/nammamart_alerts.py. A throwaway local web server stands in for Slack, so the real payload
is checked without a real webhook.

Run:  python -m pytest tests/test_nammamart_alerts.py
"""
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include"))

from nammamart_alerts import build_message, notify_run, signal_warnings  # noqa: E402

DAG = "nammamart_etl_dhananjay_vishwakarma"
SECRET_PATH = "/services/T000SECRET/B000SECRET/xxxxTOKENxxxx"
VOLUME_WARN = {"check_name": "orders.volume_drop_vs_history", "rows_checked": 50, "rows_failed": 50}
STALE_WARN = {"check_name": "orders.latest_order_not_stale", "rows_checked": 2412, "rows_failed": 2412}


@pytest.fixture()
def fake_slack():
    """Local server that records every POST body. Yields (url, received_list)."""
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}{SECRET_PATH}", received
    server.shutdown()


def test_healthy_run_needs_no_alert():
    assert build_message(DAG, "run1", [], 5212, 4765, 447, []) is None


def test_failed_run_message_names_the_failure_and_the_next_step():
    text = build_message(DAG, "run1", ["extract_products"], 5060, 0, 9, [], skipped_tasks=7)
    assert "FAILED" in text and "ETL" in text
    assert "Failed: `extract_products`" in text  # the cause comes first
    assert "Not run because of it: 7 downstream tasks" in text
    assert "extracted 5,060, loaded 0, quarantined 9" in text
    assert "Clear" in text  # tells the person how to recover


def test_message_does_not_list_every_skipped_task():
    text = build_message(DAG, "run1", ["extract_orders"], 0, 0, 0, [], skipped_tasks=15)
    assert text.count("`") == 6  # three quoted names (DAG, run, failed task), not 15 more
    assert "1 downstream task" in build_message(DAG, "r", ["x"], 0, 0, 0, [], skipped_tasks=1)


def test_warning_only_run_explains_the_reason():
    text = build_message(DAG, "run1", [], 2850, 2797, 53, [VOLUME_WARN])
    assert "data looks wrong" in text and "50 of 50 rows flagged" in text
    assert "FAILED" not in text


def test_failure_message_also_lists_warnings():
    text = build_message(DAG, "r", ["load_raw"], 1, 0, 0, [STALE_WARN])
    assert "FAILED" in text and "Also flagged" in text and "stale" in text


def test_only_signal_warnings_are_kept():
    noise = {"check_name": "orders.payment_mode_nonstandard", "rows_checked": 2412, "rows_failed": 8}
    assert signal_warnings([noise, VOLUME_WARN, STALE_WARN]) == [VOLUME_WARN, STALE_WARN]


def test_message_holds_no_customer_data():
    text = build_message(DAG, "run1", ["validate_customers"], 5212, 4765, 447, [VOLUME_WARN])
    assert "@" not in text  # no email address
    assert not re.search(r"\d{10}", text)  # no phone number: counts are written with thousands separators


def test_alert_is_posted_as_slack_json(fake_slack):
    url, received = fake_slack
    assert notify_run(DAG, "run1", ["extract_orders"], 100, 0, 0, [], webhook_url=url) is True
    assert list(received[0]) == ["text"] and "FAILED" in received[0]["text"]


def test_healthy_run_posts_nothing(fake_slack):
    url, received = fake_slack
    assert notify_run(DAG, "run1", [], 5212, 4765, 447, [], webhook_url=url) is False
    assert received == []


def test_webhook_url_is_never_printed(fake_slack, capsys):
    url, _ = fake_slack
    notify_run(DAG, "run1", ["extract_orders"], 100, 0, 0, [], webhook_url=url)
    out = capsys.readouterr().out
    assert "TOKEN" not in out and SECRET_PATH not in out


def test_without_a_webhook_the_alert_goes_to_the_log(capsys):
    assert notify_run(DAG, "run1", ["extract_orders"], 100, 0, 0, []) is False
    assert "FAILED" in capsys.readouterr().out  # printed instead of sent, nothing raised


def test_a_broken_webhook_never_raises(capsys):
    assert notify_run(DAG, "run1", ["extract_orders"], 1, 0, 0, [], webhook_url="http://127.0.0.1:9/nope") is False
    assert "nope" not in capsys.readouterr().out
