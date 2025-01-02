import datetime
import operator
import re
from base64 import urlsafe_b64decode
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List

import attr
import attrs
import pymongo

import slivka.db
from slivka import JobStatus
from slivka.consts import ServiceStatus as Status
from slivka.db.documents import JobRequest
from slivka.db.mongo_utils import date_comparison_query


@attr.s()
class ServiceStatusInfo:
    UNDEFINED = Status.UNDEFINED
    OK = Status.OK
    WARNING = Status.WARNING
    DOWN = Status.DOWN

    service = attr.ib()
    runner = attr.ib()
    status = attr.ib(converter=Status)
    message = attr.ib(default="")
    timestamp = attr.ib(factory=datetime.now)


class ServiceStatusMongoDBRepository:
    _collection = 'servicestate'

    def __init__(self, database=None):
        if database is None:
            database = slivka.db.database
        self._db = database

    def insert(self, service_status: ServiceStatusInfo):
        doc = {
            'service': service_status.service,
            'runner': service_status.runner,
            'state': service_status.status,
            'message': service_status.message,
            'timestamp': service_status.timestamp
        }
        self._db[self._collection].insert_one(doc)

    def list_all(self, service=None, runner=None) -> List[ServiceStatusInfo]:
        filters = {}
        if service is not None:
            filters['service'] = service
        if runner is not None:
            filters['runner'] = runner
        cursor = self._db[self._collection].find(filters)
        cursor = cursor.sort('timestamp', pymongo.DESCENDING)
        return [
            ServiceStatusInfo(
                service=d['service'],
                runner=d['runner'],
                status=d['state'],
                message=d['message'],
                timestamp=d['timestamp']
            )
            for d in cursor
        ]

    def list_current(self, service=None, runner=None) -> List[ServiceStatusInfo]:
        filters = {}
        if service is not None:
            filters['service'] = service
        if runner is not None:
            filters['runner'] = runner
        cursor = self._db[self._collection].aggregate([
            {'$match': filters},
            {'$sort': {'timestamp': pymongo.DESCENDING}},
            {'$group': {
                '_id': {
                    'service': '$service',
                    'runner': '$runner',
                },
                'status': {'$first': {
                    'status': '$state',
                    'message': '$message',
                    'timestamp': '$timestamp'
                }}
            }}
        ])
        return sorted(
            (
                ServiceStatusInfo(
                    service=d['_id']['service'],
                    runner=d['_id']['runner'],
                    status=d['status']['status'],
                    message=d['status']['message'],
                    timestamp=d['status']['timestamp']
                )
                for d in cursor
            ),
            key=operator.attrgetter('timestamp'),
            reverse=True
        )


ServiceStatusRepository = ServiceStatusMongoDBRepository


@attrs.define
class UsageStats:
    month: date
    service: str
    count: int


_mongodb_op_map = {
    "==": "$eq",
    ">=": "$gte",
    ">": "$gt",
    "<=": "$lte",
    "<": "$lt",
}


def _create_date_matcher(expression):
    match = re.match(r"([=><!]{1,2})(\d{4}-\d{2})$", expression)
    if match is None:
        raise ValueError(f"invalid expression {expression}")
    val = datetime.strptime(match.group(2), "%Y-%m")
    try:
        op = _mongodb_op_map[match.group(1)]
    except KeyError:
        raise ValueError(f"invalid expression {expression}")
    if op == "$eq":
        # eq means equal month, not equal date
        return {"timestamp": {"$gte": val,
                              "$lt": val + relativedelta(months=+1)}}
    elif op == "$lte":
        # lte must include entire month, not just the first of the month
        return {"timestamp": {"$lt": val + relativedelta(months=+1)}}
    elif op == "$gt":
        # gt must not include the specified month
        return {"timestamp": {"$gte": val + relativedelta(months=+1)}}
    return {"timestamp": {op: val}}


class UsageStatsMongoDBRepository:
    __requests_collection = "requests"

    def __init__(self, database=None):
        if database is None:
            database = slivka.db.database
        self._database = database

    def list_all(self, filters=()) -> List[UsageStats]:
        collection = self._database[self.__requests_collection]
        matchers = []
        for name, expr in filters:
            if name == "service":
                matchers.append({"service": expr})
            elif name == "month":
                matchers.append(_create_date_matcher(expr))
            elif name == "status":
                if expr == "completed":
                    matchers.append({"status": JobStatus.COMPLETED.value})
                elif expr == "incomplete":
                    matchers.append({"status": {"$ne": JobStatus.COMPLETED.value}})
                else:
                    raise ValueError(f"invalid expression {expr}")
            else:
                raise ValueError(f"invalid name {name}")

        pipeline = [{"$match": {"$and": matchers}}] if matchers else []
        pipeline += [
            {"$group": {
                "_id": {
                    "month": {
                        "$dateTrunc": {"date": "$timestamp", "unit": "month"}
                    },
                    "service": "$service",
                },
                "count": {"$count": {}}
            }},
            {"$sort": {"_id.month": 1}}
        ]
        return [
            UsageStats(
                month=date(
                    entry["_id"]["month"].year,
                    entry["_id"]["month"].month,
                    1
                ),
                service=entry["_id"]["service"],
                count=entry["count"]
            )
            for entry in collection.aggregate(pipeline)
        ]


UsageStatsRepository = UsageStatsMongoDBRepository


class RequestsMongoDBRepository:
    __requests_collection = "requests"

    def __init__(self, database=None):
        if database is None:
            database = slivka.db.database
        self._database = database

    def list(self, filters=(), limit=0, skip=0):
        """
        Fetch a list of :py:class:`JobRequest`s that satisfy ``filters``
        criteria. The returned elements are sorted by the request creation
        date, newest first.

        The filters should be provided as a list of  ``(name, value)`` tuples.
        Multiple filters are combined with an *and* operator. The available
        filters are *id*, *service*, *submissionTime* and *status*.

        The **submissionTime** filter consist of date and time in the ISO 8601
        format optionally preceded by a comparison operator e.g. ``>2024-03``
        or ``<=2022-05-12T12:00``. No operator is equivalent to an equality
        operator.

        :param filters: the list of filter rules
        :type filters: list[tuple[str, Any]]
        :param limit: limit the number of results, or 0 for no limit
        :param skip: number of results to skip from the beginning
        :return: list of requests meeting the criteria
        """
        collection = self._database[self.__requests_collection]
        matchers = []
        for name, value in filters:
            if name == "id":
                matchers.append({"_id": urlsafe_b64decode(value)})
            elif name == "service":
                matchers.append({"service": value})
            elif name == "submissionTime":
                matchers.append({"timestamp": date_comparison_query(value)})
            elif name == "status":
                matchers.append({"status": value})
            else:
                raise ValueError(f"invalid filter key: {name}")
        query = {"$and": matchers} if matchers else {}
        cursor = collection.find(query, sort={"timestamp": -1}, limit=limit, skip=skip)
        return [JobRequest(**kw) for kw in cursor]


RequestsRepository = RequestsMongoDBRepository
