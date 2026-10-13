"""
include/nammamart_alerts.py
---------------------------
Slack alerts for the NammaMart ETL and ELT DAGs.

An alert is sent when
  * any task of the run failed, or
  * the run succeeded but a check that signals missing or old data warned (volume drop, stale feed).
A clean run sends nothing, so the channel stays quiet until something needs a person.

The webhook URL is a secret. It is read from the Airflow Variable SLACK_WEBHOOK_URL (Admin -> Variables, or
`airflow variables set`), never from code, and is never printed. With no Variable set the alert is printed to
the task log instead, so the pipelines run the same without Slack.

Optional Variable NAMMAMART_ALERT_ON_WARNINGS ("false" turns off warning alerts; failures always alert).

Messages hold only counts, task names and check names: no customer data.
"""

from __future__ import annotations

from slack_notifier import send_slack_alert  # the trainer's helper: posts {"text": ...} and never prints the URL

# Warnings that mean "the data itself looks wrong". Other warnings (casing, odd formats) are normal noise.
SIGNAL_WARNINGS = {
    "orders.volume_drop_vs_history": "the orders file has over 90% fewer rows than the largest earlier file",
    "orders.latest_order_not_stale": "the newest order is more than 1 day old, so the feed looks stale",
}


def _variable(name: str, default=None):
    """Read an Airflow Variable. Falls back to the default outside Airflow (for example in unit tests)."""
    try:
        from airflow.sdk import Variable
        return Variable.get(name, default=default)
    except Exception:
        return default


def _pipeline_name(dag_id: str) -> str:
    return "ELT" if "_elt_" in dag_id else "ETL" if "_etl_" in dag_id else dag_id


def signal_warnings(dq_rows: list[dict]) -> list[dict]:
    """Keep only the WARN rows of the checks in SIGNAL_WARNINGS. Each row: check_name, rows_checked, rows_failed."""
    return [r for r in dq_rows if r["check_name"] in SIGNAL_WARNINGS]


def warnings_from_warehouse(db_path: str, dag_id: str, run_id: str) -> list[dict]:
    """The signal WARN rows that dq_results recorded for one run. Empty if the table or run is missing."""
    try:
        import duckdb
        con = duckdb.connect(db_path, read_only=True)
        try:
            rows = con.execute("""SELECT check_name, rows_checked, rows_failed FROM dq_results
                                  WHERE run_id = ? AND dag_id = ? AND status = 'WARN'""", [run_id, dag_id]).fetchall()
        finally:
            con.close()
    except Exception:
        return []
    return signal_warnings([{"check_name": c, "rows_checked": n, "rows_failed": f} for c, n, f in rows])


def build_message(dag_id: str, run_id: str, failed_tasks: list[str], rows_extracted: int, rows_loaded: int,
                  rows_quarantined: int, warnings: list[dict], skipped_tasks: int = 0) -> str | None:
    """The Slack text, or None when the run needs nobody's attention.
    failed_tasks are the tasks that really failed (the cause). skipped_tasks counts the downstream tasks that did
    not run because of them, so the message leads with the cause instead of a wall of task names."""
    pipeline = _pipeline_name(dag_id)
    counts = f"extracted {rows_extracted:,}, loaded {rows_loaded:,}, quarantined {rows_quarantined:,}"
    warning_lines = [f"• {SIGNAL_WARNINGS[w['check_name']]} ({w['rows_failed']:,} of {w['rows_checked']:,} rows flagged)"
                     for w in warnings]

    if failed_tasks:
        lines = [
            f":red_circle: *NammaMart {pipeline} run FAILED*",
            f"• DAG: `{dag_id}`",
            f"• Run: `{run_id}`",
            f"• Failed: {', '.join(f'`{t}`' for t in failed_tasks)}",
        ]
        if skipped_tasks:
            lines.append(f"• Not run because of it: {skipped_tasks} downstream task{'s' if skipped_tasks != 1 else ''}")
        lines.append(f"• Rows: {counts}")
        if warning_lines:
            lines += ["Also flagged:"] + warning_lines
        lines.append("Next step: open the run in Airflow, read the failed task's log, fix the cause, then Clear the "
                     "failed task. Only that task and what follows will re-run.")
        return "\n".join(lines)

    if warning_lines:
        return "\n".join([
            f":warning: *NammaMart {pipeline} run finished, but the data looks wrong*",
            f"• DAG: `{dag_id}`",
            f"• Run: `{run_id}`",
            f"• Rows: {counts}",
            "Reasons:", *warning_lines,
            "The reporting tables were built, so check the source files before the 9:30 stand-up.",
        ])
    return None


def notify_run(dag_id: str, run_id: str, failed_tasks: list[str], rows_extracted: int, rows_loaded: int,
               rows_quarantined: int, warnings: list[dict], webhook_url: str | None = None,
               skipped_tasks: int = 0) -> bool:
    """Send the alert if the run needs one. Returns True only if Slack accepted it. Never raises: an alert
    problem must not turn a good run into a failed one."""
    try:
        if not failed_tasks and str(_variable("NAMMAMART_ALERT_ON_WARNINGS", "true")).lower() == "false":
            warnings = []
        message = build_message(dag_id, run_id, failed_tasks, rows_extracted, rows_loaded, rows_quarantined, warnings,
                                skipped_tasks)
        if message is None:
            print("[Alert] run is healthy, nothing to send")
            return False
        url = webhook_url or _variable("SLACK_WEBHOOK_URL")
        return bool(send_slack_alert(message, webhook_url=url))  # prints the message instead when url is empty
    except Exception as exc:
        print(f"[Alert] could not send the alert: {type(exc).__name__}")
        return False
