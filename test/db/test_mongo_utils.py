from datetime import datetime

import pytest
from dateutil.relativedelta import relativedelta

from slivka.db.mongo_utils import date_cmp_regex, date_comparison_query


@pytest.mark.parametrize(
    "operator", ["=", "<", ">", "<=", ">="]
)
@pytest.mark.parametrize(
    ("date_expr", "expected_dict"),
    [
        ("2022", {"year": "2022", "month": None, "day": None, "hour": None, "minute": None, "second": None}),
        ("2022-11", {"year": "2022", "month": "11", "day": None, "hour": None, "minute": None, "second": None}),
        ("2024-02", {"year": "2024", "month": "02", "day": None, "hour": None, "minute": None, "second": None}),
        ("2024-11-24", {"year": "2024", "month": "11", "day": "24", "hour": None, "minute": None, "second": None}),
        ("2024-02-03", {"year": "2024", "month": "02", "day": "03", "hour": None, "minute": None, "second": None}),
        ("2024-02-24T03", {"year": "2024", "month": "02", "day": "24", "hour": "03", "minute": None, "second": None}),
        ("2005-04-02T21:37", {"year": "2005", "month": "04", "day": "02", "hour": "21", "minute": "37", "second": None}),
        ("2025-04-02T23:59:59", {"year": "2025", "month": "04", "day": "02", "hour": "23", "minute": "59", "second": "59"})
    ]
)
def test_date_cmp_regex_parse_date(operator, date_expr, expected_dict):
    m = date_cmp_regex.match(operator + date_expr)
    assert m.groupdict() == expected_dict


@pytest.mark.parametrize("operator", ["=", "<", ">", "<=", ">=", "", "=="])
def test_date_cmp_regex_operator_capture(operator):
    m = date_cmp_regex.match(operator + "2024-12-16T11:38")
    assert m.group(1) == (operator if operator != '' else None)


@pytest.mark.parametrize(
    "date_expr",
    [
        "2022T03",
        "2024-10T12:04",
        "24-12-30",
        "16:18:20",
        "2024-01-01-01",
    ]
)
def test_invalid_date_expression(date_expr):
    m = date_cmp_regex.match(date_expr)
    assert m is None


@pytest.mark.parametrize(
    ("date_expr", "inserted_dates", "expected_result"),
    [
        (
            "2024",
            ["2023-12-01T21:20:19", "2024-01-02T21:20:19", "2024-12-31T23:59:59", "2025-01-01T00:00:00"],
            ["2024-01-02T21:20:19", "2024-12-31T23:59:59"]
        ),
        (
            ">2024-03",
            ["2023-11-01", "2024-02-01", "2024-03-31", "2024-04-01", "2025-01-01"],
            ["2024-04-01", "2025-01-01"]
        ),
        (
            ">=2024-03",
            ["2023-11-01", "2024-02-01", "2024-03-31", "2024-04-01", "2025-01-01"],
            ["2024-03-31", "2024-04-01", "2025-01-01"]
        ),
        (
            "=2024-03-21",
            ["2023-03-21T10:00", "2024-03-20T23:59", "2024-03-21T00:00", "2024-03-21T23:59", "2024-03-22T01:00"],
            ["2024-03-21T00:00", "2024-03-21T23:59"]
        ),
        (
            "<=2023-04-01",
            ["2023-03-31T23:59:59", "2023-04-01T00:00:00", "2023-04-01T00:00:01", "2023-04-01T23:59", "2023-04-02T00:00"],
            ["2023-03-31T23:59:59", "2023-04-01T00:00:00", "2023-04-01T00:00:01", "2023-04-01T23:59"]
        )
    ]
)
def test_date_comparison_query(database, date_expr, inserted_dates, expected_result):
    inserted_dates = (datetime.fromisoformat(d) for d in inserted_dates)
    expected_result = [datetime.fromisoformat(d) for d in expected_result]
    collection = database["dates_collection"]
    collection.insert_many([{"date": date} for date in inserted_dates])
    query = date_comparison_query(date_expr)
    results = list(collection.find({"date": query}))
    assert [r['date'] for r in results] == expected_result
