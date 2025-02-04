import itertools

import pymongo.database
import pytest

from datetime import datetime, timezone, tzinfo

import slivka.migrations.migration_2
from slivka import JobStatus


@pytest.fixture
def job_requests(database: pymongo.database.Database, submission_times, completion_times):
    requests = [
        {
            "service": "example",
            "inputs": {},
            "timestamp": sub_time,
            "completion_time": compl_time,
            "status": JobStatus.COMPLETED,
            "runner": None,
            "job": None
        }
        for sub_time, compl_time in zip(submission_times, completion_times)
    ]
    database['requests'].insert_many(requests)
    return requests


@pytest.mark.parametrize(
    "completion_times",
    [itertools.repeat(None)]
)
@pytest.mark.parametrize(
    "submission_times",
    [
        [datetime(2024, 2, 3, 16), datetime(2024, 2, 8, 15)],
        [datetime(2005, 8, 12, 8, 35)],
        [datetime(2024, 5, 1, 0)],
        [datetime(2024, 5, 31, 23)],
        [datetime(1999, 11, 1, 0)],
        [datetime(2024, 11, 30, 23)],
    ]
)
def test_submission_time_fix(database, job_requests, submission_times, completion_times):
    slivka.migrations.migration_2.apply()
    data = list(database['requests'].find())
    expected = [dt.astimezone(timezone.utc).replace(tzinfo=None) for dt in submission_times]
    actual = [d["timestamp"] for d in data]
    assert actual == expected


@pytest.mark.parametrize(
    "submission_times",
    [itertools.repeat(datetime(2024, 1, 1, 12))]
)
@pytest.mark.parametrize(
    "completion_times",
    [
        [datetime(2024, 2, 3, 16), datetime(2024, 2, 8, 15)],
        [datetime(2025, 8, 12, 8, 35)],
        [datetime(2024, 5, 1, 0)],
        [datetime(2024, 5, 31, 23)],
        [datetime(2024, 11, 1, 0)],
        [datetime(2024, 11, 30, 23)],
    ]
)
def test_completion_time_fix(database, job_requests, submission_times, completion_times):
    slivka.migrations.migration_2.apply()
    data = list(database['requests'].find())
    expected = [dt.astimezone(timezone.utc).replace(tzinfo=None) for dt in completion_times]
    actual = [d['completion_time'] for d in data]
    assert actual == expected
