from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pytest

from slivka.migrations import migration_2_tz_aware_datetimes



UTC = timezone.utc


@pytest.mark.parametrize(
    ('timestamps', 'expected_ts', 'from_tz'),
    [
        pytest.param(
            [
                datetime(2025, 3, 12, 10, 37),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            [
                datetime(2025, 3, 12, 10, 37),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            UTC,
            id='from UTC'
        ),
        pytest.param(
            [
                datetime(2025, 3, 12, 10, 37),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            [
                dt.astimezone(UTC).replace(tzinfo=None)
                for dt in [
                    datetime(2025, 3, 12, 10, 37),
                    datetime(2025, 7, 13, 11, 21),
                    datetime(2025, 11, 14, 21, 5)
                 ]
            ],
            None,
            id='from local'
        ),
        pytest.param(
            [
                datetime(2025, 3, 12, 4, 37),
                datetime(2025, 3, 12, 23, 59),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            [
                datetime(2025, 3, 12, 11, 37),
                datetime(2025, 3, 13, 6, 59),
                datetime(2025, 7, 13, 18, 21),
                datetime(2025, 11, 15, 4, 5)
            ],
            timezone(timedelta(hours=-7)),
            id='from fixed offset'
        ),
        pytest.param(
    [
                datetime(2025, 3, 12, 4, 37),
                datetime(2025, 3, 12, 23, 59),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            [
                datetime(2025, 3, 12, 11, 37),
                datetime(2025, 3, 13, 6, 59),
                datetime(2025, 7, 13, 18, 21),
                datetime(2025, 11, 15, 5, 5)
            ],
            ZoneInfo("America/Los_Angeles"),
            id='from Los Angeles'
        ),
        pytest.param(
            [
                datetime(2025, 3, 12, 10, 37),
                datetime(2025, 7, 13, 11, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            [
                datetime(2025, 3, 12, 10, 37),
                datetime(2025, 7, 13, 10, 21),
                datetime(2025, 11, 14, 21, 5)
            ],
            ZoneInfo("Europe/London"),
            id='from London'
        ),
    ]
)
def test_migrate_dates(database, timestamps, expected_ts, from_tz):
    job_request_base = {
        'service': 'example',
        'inputs': {},
        'timestamp': datetime.fromtimestamp(0),
        'completion_time': None,
        'status': 1,
        'runner': None,
        'job': None
    }
    database.requests.insert_many([
        {**job_request_base, 'timestamp': timestamp}
        for timestamp in timestamps
    ])
    migration_2_tz_aware_datetimes.apply(database, from_tz)
    actual_ts = [
        it['timestamp'] for it in database.requests.find()
    ]
    assert actual_ts == expected_ts
