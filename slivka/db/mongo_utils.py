import datetime
import re

from dateutil.relativedelta import relativedelta

_op_map = {
    None: "$eq",
    "=": "$eq",
    "==": "$eq",
    ">=": "$gte",
    ">": "$gt",
    "<=": "$lte",
    "<": "$lt",
}

date_cmp_regex = re.compile(
    r"([=><]{1,2})?"
    r"(?P<year>\d\d\d\d)"
    r"(?:-(?P<month>\d\d)"
    r"(?:-(?P<day>\d\d)"
    r"(?:T(?P<hour>\d\d)"
    r"(?::(?P<minute>\d\d)"
    r"(?::(?P<second>\d\d)"
    r")?)?)?)?)?$"
)


def date_comparison_query(expression):
    """Builds mongodb query for comparing dates

    >>> date_comparison_query("<2024-03-13")
    {'$lt': datetime.datetime(2024, 3, 13, 0, 0)}

    >>> date_comparison_query("<=2024-05-19T21")
    {'$lt': datetime.datetime(2024, 5, 19, 22, 0)}
    """
    match = date_cmp_regex.match(expression)
    if match is None:
        raise ValueError(f"invalid date expression {expression}")
    try:
        operator = _op_map[match.group(1)]
    except KeyError:
        raise ValueError(f"invalid date expression {expression}")
    datetime_dict = {
        key: int(val) for key, val in match.groupdict().items()
        if val is not None
    }
    resolution = (
        "seconds" if "second" in datetime_dict else
        "minutes" if "minute" in datetime_dict else
        "hours" if "hour" in datetime_dict else
        "days" if "day" in datetime_dict else
        "months" if "month" in datetime_dict else
        "years" if "year" in datetime_dict else
        ValueError
    )
    assert isinstance(resolution, str), "error processing datetime"
    # datetime.datetime requires month and day
    datetime_dict.setdefault("month", 1)
    datetime_dict.setdefault("day", 1)
    value = datetime.datetime(**datetime_dict)
    # comparisons must be no more accurate than the date resolution
    if operator == "$eq":
        # e.g. =YYYY-mm means YYYY-mm-01T00:00:00 <= date <= YYYY-mm-31T23:59:59
        # which is the same as YYYY-mm-01T00:00:00 <= date < YYYY-(mm+1)-01T00:00:00
        return {"$gte": value, "$lt": value + relativedelta(**{resolution: +1})}
    if operator == "$lte":
        # e.g. <=YYYY-mm means date <= YYYY-mm-31T23:59:59
        # which is the same as date < YYYY-(mm+1)T00:00:00
        return {"$lt": value + relativedelta(**{resolution: +1})}
    if operator == "$gt":
        # e.g. >YYYY-mm means date > YYYY-mm-31T23:59:59
        # which is the same as date >= YYYY-(mm+1)-01T00:00:00
        return {"$gte": value + relativedelta(**{resolution: +1})}
    # other operators don't need rewriting
    return {operator: value}
