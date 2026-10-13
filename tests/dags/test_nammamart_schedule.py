"""
Pins down Part C: both NammaMart DAGs run at 09:00 IST, Monday to Saturday, with explicit timezone,
catchup off, and retry settings. Run inside the Airflow image:  astro dev pytest tests/dags/test_nammamart_schedule.py
"""
import os
from datetime import timedelta

import pendulum
import pytest
from airflow.models import DagBag
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import DagSerialization
from airflow.timetables.base import DataInterval, TimeRestriction

IST = "Asia/Kolkata"
DAG_IDS = ["nammamart_etl_dhananjay_vishwakarma", "nammamart_elt_dhananjay_vishwakarma"]
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def scheduler_view(dag):
    """The scheduler works on the serialized DAG, so test that one."""
    return DagSerialization.from_dict(DagSerialization.to_dict(dag))


def next_runs(timetable, count, earliest, catchup=False, last=None):
    runs = []
    for _ in range(count):
        info = timetable.next_dagrun_info(
            last_automated_data_interval=last,
            restriction=TimeRestriction(earliest=earliest, latest=None, catchup=catchup))
        runs.append(info.run_after)
        last = info.data_interval
    return runs


def test_dags_parse_without_errors(dagbag):
    assert dagbag.import_errors == {}
    for dag_id in DAG_IDS:
        assert dag_id in dagbag.dags


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_runs_at_0900_ist_monday_to_saturday(dagbag, dag_id):
    timetable = scheduler_view(dagbag.dags[dag_id]).timetable
    runs = next_runs(timetable, 14, pendulum.datetime(2026, 9, 1, tz=IST))
    for run in runs:
        local = run.in_timezone(IST)
        assert (local.hour, local.minute) == (9, 0)
        assert local.day_of_week != pendulum.SUNDAY  # Monday to Saturday only
        assert run.in_timezone("UTC").format("HH:mm") == "03:30"  # what the Airflow UI shows
    assert pendulum.SATURDAY in [r.in_timezone(IST).day_of_week for r in runs]  # Saturday does run


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_start_date_is_timezone_aware_ist(dagbag, dag_id):
    start = dagbag.dags[dag_id].start_date
    assert start.tzinfo is not None  # never a naive datetime
    # Airflow stores it in UTC: 2026-08-31 18:30 UTC is exactly 2026-09-01 00:00 IST
    assert start == pendulum.datetime(2026, 9, 1, tz=IST)


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_catchup_off_and_one_run_at_a_time(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert dag.catchup is False
    assert dag.max_active_runs == 1  # DuckDB allows a single writer


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_retry_settings(dagbag, dag_id):
    args = dagbag.dags[dag_id].default_args
    assert args["retries"] == 2
    assert args["retry_delay"] == timedelta(seconds=30)
    assert args["execution_timeout"] == timedelta(minutes=2)
    worst_case = 3 * args["execution_timeout"] + 2 * args["retry_delay"]
    assert worst_case < timedelta(minutes=30)  # stays inside the 09:00 to 09:30 window


def test_missed_days_after_downtime_run_only_the_latest_slot(dagbag):
    timetable = scheduler_view(dagbag.dags[DAG_IDS[0]]).timetable
    last = DataInterval(pendulum.datetime(2026, 10, 8, 3, 30, tz="UTC"), pendulum.datetime(2026, 10, 8, 3, 30, tz="UTC"))
    earliest = pendulum.datetime(2026, 9, 1, tz=IST)
    (only,) = next_runs(timetable, 1, earliest, catchup=False, last=last)
    assert only.in_timezone(IST).format("YYYY-MM-DD") == "2026-10-13"  # Tue: skips Fri, Sat, Mon
    replay = next_runs(timetable, 4, earliest, catchup=True, last=last)
    assert [r.in_timezone(IST).format("ddd") for r in replay] == ["Fri", "Sat", "Mon", "Tue"]


def test_a_utc_start_date_would_run_at_1430_ist():
    """The mistake the brief warns about: the same cron string with a UTC start_date fires 5.5 hours late."""
    dag = DAG("probe_utc", schedule="0 9 * * 1-6", start_date=pendulum.datetime(2026, 9, 1, tz="UTC"))
    run = next_runs(scheduler_view(dag).timetable, 1, pendulum.datetime(2026, 9, 1, tz="UTC"))[0]
    assert run.in_timezone(IST).format("HH:mm") == "14:30"
