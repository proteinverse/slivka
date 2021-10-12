import contextlib
import logging
import operator
import os
import threading
from collections import defaultdict, namedtuple, OrderedDict
from datetime import datetime
from functools import partial
from operator import itemgetter
from typing import (Iterable, Tuple, Dict, List, Any, Union, DefaultDict,
                    Sequence, Callable)

import pymongo.errors
from bson import ObjectId
from pymongo import UpdateOne

from db.helpers import delete_many
from slivka.utils import retry_call

import slivka.conf
import slivka.db
from slivka.db.documents import (JobRequest, JobMetadata, CancelRequest,
                                 ServiceState)
from slivka.db.helpers import insert_many, push_one, insert_one
from slivka.utils import JobStatus, BackoffCounter
from .runners import Job as JobTuple
from .runners.runner import RunnerID, Runner


def get_classpath(cls):
    return cls.__module__ + '.' + cls.__name__


RunResult = namedtuple('RunResult', 'started, deferred, failed')

# sentinel valued corresponding to the rejected and error requests
REJECTED = object()
ERROR = object()


class _ServiceStateHelper:
    def __init__(self, database=None):
        self._db = database or slivka.db.database
        self._states = {}  # type: dict[tuple, ServiceState]
        self._ids = {}

    def update_state(self, service, runner, tag, state, message):
        current = self._get_service_state(service, runner, tag)
        if current.state != state or current.message != message:
            current.reset_timestamp()
            current.message = message
            current.state = state
            filter_key = (service, runner)
            most_severe = max(
                (v for k, v in self._states.items() if k[:2] == filter_key),
                key=operator.attrgetter('state')
            )
            push_one(self._db, most_severe)

    def _get_service_state(self, service, runner, tag) -> ServiceState:
        try:
            return self._states[service, runner, tag]
        except KeyError:
            _id = self._get_database_id(service, runner)
            state = ServiceState(_id=_id, service=service, runner=runner)
            self._states[service, runner, tag] = state
            return state

    def _get_database_id(self, service, runner) -> ObjectId:
        state = ServiceState.find_one(self._db, service=service, runner=runner)
        if state is None:
            state = ServiceState(service=service, runner=runner)
            insert_one(self._db, state)
        return state.id


class Scheduler:
    """
    Scheduler is a central hub of the slivka system. It runs in it's
    individual process and manages jobs.

    In it's main loot, it first
    fetches the pending job requests from the database and sorts them
    to available runners using provided limiters (see :py:class:`Limiter`)
    for decision making. Then, the accepted requests are being executed
    with the runner and the created jobs are stored in the database.

    In the next stage, the scheduler checks all currently running jobs
    and updates their state into the database.
    """

    def __init__(self, jobs_directory=None):
        self.log = logging.getLogger(__name__)
        self._finished = threading.Event()
        self.jobs_directory = jobs_directory or slivka.conf.settings.directory.jobs
        self.runners: Dict[RunnerID, Runner] = {}
        self.selectors: Dict[str, Callable] = defaultdict(lambda: BaseSelector.default)
        self._backoff_counters: DefaultDict[Any, BackoffCounter] = \
            defaultdict(partial(BackoffCounter, max_tries=10))
        self._service_states = _ServiceStateHelper()

    @property
    def is_running(self):
        """ Checks whether the scheduler is running. """
        return not self._finished.is_set()

    def set_failure_limit(self, limit):
        """ Sets the limit of allowed exceptions before job is rejected. """
        factory = partial(BackoffCounter, max_tries=limit)
        self._backoff_counters.default_factory = factory
        for counter in self._backoff_counters.values():
            counter.max_tries = limit

    def add_runner(self, runner: Runner):
        self.runners[runner.id] = runner

    def add_selector(self, service: str, selector: Callable):
        self.selectors[service] = selector

    def stop(self):
        self._finished.set()

    def run_forever(self):
        """ Starts the main loop

        The main loop is running until the :py:meth:`stop` is called.
        It repeatedly performs work cycles with one second delay
        between them.
        """
        if self._finished.is_set():
            raise RuntimeError("scheduler can only be started once")
        self.log.info('scheduler started')
        try:
            while not self._finished.wait(1):
                self.main_loop()
        except KeyboardInterrupt:
            self.stop()

    def main_loop(self):
        database = slivka.db.database

        # fetching new requests
        self._assign_runners(database)

        # Fetch cancel requests and update cancelled job states to
        # DELETED if they were PENDING or ACCEPTED; or CANCELLING if QUEUED or RUNNING
        self._stop_cancelled(database)

        # starting ACCEPTED requests
        self._run_accepted(database)

        # monitoring jobs
        self._update_running(database)

    def _assign_runners(self, database):
        """Assigns new status and runner to pending requests.

        For each pending request in the database, uses selector
        to find the appropriate runner or gives a REJECTED or ERROR status
        """
        log = self.log
        auto_reconnect_handler = partial(_auto_reconnect_handler, log)
        new_requests = retry_call(
            partial(_fetch_pending_requests, database),
            pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
        )
        grouped = self.group_requests(new_requests)
        rejected = grouped.pop(REJECTED, ())
        if rejected:
            retry_call(
                partial(_bulk_set_status, database, rejected, JobStatus.REJECTED),
                pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )
        error = grouped.pop(ERROR, ())
        if error:
            retry_call(
                partial(_bulk_set_status, database, error, JobStatus.ERROR),
                pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )
        for runner, requests in grouped.items():
            retry_call(
                partial(_bulk_set_accepted, database, requests, runner),
                pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )

    def group_requests(self, requests: Iterable[JobRequest]) \
            -> Dict[Union[Runner, object], List[JobRequest]]:
        """Group requests to their corresponding runners or reject."""
        grouped = defaultdict(list)
        for request in requests:
            selector = self.selectors[request.service]
            runner_name = selector(request.inputs)
            if runner_name is None:
                grouped[REJECTED].append(request)
            else:
                try:
                    runner = self.runners[request.service, runner_name]
                    grouped[runner].append(request)
                except KeyError:
                    grouped[ERROR].append(request)
                    self.log.exception(
                        "runner \"%s\" does not exist for service \"%s\"",
                        runner_name, request.service
                    )
        return grouped

    def _stop_cancelled(self, database):
        auto_reconnect_handler = partial(_auto_reconnect_handler, self.log)
        cancel_requests = retry_call(
            partial(_fetch_cancel_requests, database),
            exceptions=pymongo.errors.AutoReconnect,
            handler=auto_reconnect_handler
        )
        if cancel_requests:
            job_ids = [cr.job_id for cr in cancel_requests]
            fn = partial(_bulk_set_status_filter_by_status, database, job_ids,
                         [JobStatus.PENDING, JobStatus.ACCEPTED], JobStatus.DELETED)
            retry_call(
                fn, pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )
            fn = partial(_bulk_set_status_filter_by_status, database, job_ids,
                         [JobStatus.QUEUED, JobStatus.RUNNING], JobStatus.CANCELLING)
            retry_call(
                fn, pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )
            cancelled_jobs = retry_call(
                partial(_fetch_job_metadatas, database, job_ids),
                pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )
            for job in cancelled_jobs:
                # fixme: do not blindly trust data from the database
                #        the runner may not exist
                runner = self.runners[job.service, job.runner]
                with contextlib.suppress(OSError):
                    runner.cancel(JobTuple(job.job_id, job.cwd))
            retry_call(
                partial(delete_many, database, cancel_requests),
                pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
            )

    def _run_accepted(self, database):
        auto_reconnect_handler = partial(_auto_reconnect_handler, self.log)
        items = retry_call(
            partial(_fetch_accepted_requests, database),
            pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
        )
        for item in items:
            requests = [JobRequest(**kw) for kw in item['requests']]
            try:
                runner = self.runners[RunnerID(**item['_id'])]
            except KeyError:
                self.log.exception("Runner not found.")
                failed = requests
            else:
                started, deferred, failed = self.run_requests(runner, requests)
                new_jobs = []
                queued = []
                for request, job in started:
                    queued.append(request)
                    new_jobs.append(JobMetadata(
                        _id=request.id,
                        service=request.service,
                        runner=runner.name,
                        job_id=job.id,
                        work_dir=job.cwd,
                        status=JobStatus.QUEUED
                    ))
                if new_jobs:
                    retry_call(
                        partial(insert_many, database, new_jobs),
                        pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
                    )
                    retry_call(
                        partial(_bulk_set_status, database, queued, JobStatus.QUEUED),
                        pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
                    )
            if failed:
                retry_call(
                    partial(_bulk_set_status, database, failed, JobStatus.ERROR),
                    pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
                )

    def run_requests(self, runner: Runner, requests: List[JobRequest]) \
            -> RunResult:
        """ Run all requests with the runner provided.

        Runs all the job requests using the supplied implementation
        of :py:class:`Runner` and returns a three-element tuple
        containing three groups of requests: started, deferred and failed.
        The "started" group contains an iterable of :py:class:`JobRequest`
        and :py:class:`RunInfo` pairs corresponding to the successfully
        started jobs. The "deferred" group contains requests delayed due
        to failure. The "failed" group contains requests that failed
        multiple run attempts and should not be repeated.
        """
        # FIXME: this method should have cleaner implementation
        counter = self._backoff_counters[runner.start]
        if not requests or next(counter) > 0:
            return RunResult(started=(), deferred=requests, failed=())
        try:
            jobs = runner.batch_start(
                [req.inputs for req in requests],
                [os.path.join(self.jobs_directory, req.b64id) for req in requests]
            )
            self._service_states.update_state(
                runner.service_name, runner.name, 'start', ServiceState.OK, 'OK'
            )
            return RunResult(
                started=zip(requests, jobs), deferred=(), failed=()
            )
        except OSError as e:
            self.log.exception("Running %s requests failed.", runner)
            counter.failure()
            if counter.give_up:
                state = ServiceState.State.DOWN
                result = RunResult(started=(), deferred=(), failed=requests)
            else:
                state = ServiceState.State.WARNING
                result = RunResult(started=(), deferred=requests, failed=())
            self._service_states.update_state(
                runner.service_name, runner.name, 'start', state, str(e)
            )
            return result

    def _update_running(self, database):
        auto_reconnect_handler = partial(_auto_reconnect_handler, self.log)
        items = retry_call(
            partial(_fetch_running_jobs, database),
            pymongo.errors.AutoReconnect, handler=auto_reconnect_handler
        )
        for item in items:
            _id = RunnerID(**item['_id'])
            jobs = [JobMetadata(**kw) for kw in item['jobs']]
            ts = datetime.now()
            try:
                runner = self.runners[_id]
            except KeyError:
                self.log.exception("Runner (%s, %s) does not exist",
                                   _id.service, _id.runner)
                updated = [(job, JobStatus.ERROR) for job in jobs]
            else:
                updated = self.monitor_jobs(runner, jobs)
            if not updated:
                continue
            while True:
                try:
                    JobRequest.collection(database).bulk_write([
                        UpdateOne({'_id': job.id},
                                  {'$set': {'status': status, 'completion_time': ts}})
                        for (job, status) in updated
                    ], ordered=False)
                except pymongo.errors.AutoReconnect as e:
                    auto_reconnect_handler(e)
                else:
                    break
            while True:
                try:
                    JobMetadata.collection(database).bulk_write([
                        UpdateOne({'_id': job.id}, {'$set': {'status': status}})
                        for (job, status) in updated
                    ], ordered=False)
                except pymongo.errors.AutoReconnect as e:
                    auto_reconnect_handler(e)
                else:
                    break

    def monitor_jobs(self, runner: Runner, jobs: List[JobMetadata]) \
            -> Sequence[Tuple[JobMetadata, JobStatus]]:
        """ Checks status of jobs.

        Checks status of jobs using the provided runner or runner class
        and returns a list of those, whose status have changed.
        """
        counter = self._backoff_counters[runner.check_status]
        if not jobs or next(counter) > 0:
            return ()
        try:
            statuses = runner.batch_check_status([JobTuple(j.job_id, j.cwd) for j in jobs])
            results = [(job, status) for (job, status)
                       in zip(jobs, statuses) if job.status != status]
            if results and all(v[1] == JobStatus.ERROR for v in results):
                self.log.exception(
                    "Jobs of %s exited with error state", runner)
                counter.failure()
                service_state = ServiceState.DOWN
                service_message = 'All jobs reported error status.'
            else:
                service_state = ServiceState.OK
                service_message = 'OK'
        except OSError as e:
            self.log.exception("Checking job status for %s failed.", runner)
            counter.failure()
            if counter.give_up:
                results = [(job, JobStatus.ERROR) for job in jobs]
                service_state = ServiceState.DOWN
            else:
                results = ()
                service_state = ServiceState.WARNING
            service_message = str(e)
        self._service_states.update_state(
            runner.service_name, runner.name, 'state', service_state,
            service_message
        )
        return results


def _auto_reconnect_handler(log, exception):
    assert isinstance(exception, pymongo.errors.AutoReconnect)
    log.exception("Could not connect to mongo server.", exc_info=True)


def _fetch_pending_requests(database) -> Iterable[JobRequest]:
    requests = (JobRequest
                .collection(database)
                .find({'status': JobStatus.PENDING}))
    return [JobRequest(**kwargs) for kwargs in requests]


def _bulk_set_status(database, requests, status):
    JobRequest.collection(database).update_many(
        {'_id': {'$in': [req.id for req in requests]}},
        {'$set': {'status': status}}
    )


def _bulk_set_accepted(database, requests, runner):
    JobRequest.collection(database).update_many(
        {'_id': {'$in': [req.id for req in requests]}},
        {'$set': {
            'status': JobStatus.ACCEPTED,
            'runner': runner.name
        }}
    )


def _fetch_job_metadatas(database, job_ids):
    return list(JobMetadata.find(database, {'_id': {'$in': job_ids}}))


def _fetch_cancel_requests(database) -> List[CancelRequest]:
    return list(CancelRequest.find(database))


def _bulk_set_status_filter_by_status(database, job_ids, from_statuses, to_status):
    JobRequest.collection(database).update_many(
        {'_id': {'$in': job_ids},
         'status': {'$in': from_statuses}},
        {'$set': {'status': to_status}}
    )


def _fetch_accepted_requests(database):
    return list(JobRequest.collection(database).aggregate([
        {'$match': {'status': JobStatus.ACCEPTED}},
        {'$group': {
            '_id': {'service': '$service',
                    'runner': '$runner'},
            'requests': {'$push': '$$CURRENT'}
        }}
    ]))


def _fetch_running_jobs(database):
    return list(JobMetadata.collection(database).aggregate([
        {'$match': {
            'status': {'$in': (JobStatus.QUEUED, JobStatus.RUNNING)}
        }},
        {'$group': {
            '_id': {'service': '$service',
                    'runner': '$runner'},
            'jobs': {'$push': '$$CURRENT'}
        }}
    ]))


class IntervalThread(threading.Thread):
    def __init__(self, interval, target,
                 name=None, args=None, kwargs=None):
        threading.Thread.__init__(self, name=name)
        self._target = target
        self._args = args or ()
        self._kwargs = kwargs or {}
        self.interval = interval
        self._finished = threading.Event()

    def cancel(self):
        """Stop the interval thread."""
        self._finished.set()

    def run(self) -> None:
        args, kwargs = self._args, self._kwargs
        try:
            while not self._finished.wait(self.interval):
                self._target(*args, **kwargs)
        finally:
            self._finished.set()
            del self._target, self._args, self._kwargs


class SelectorMeta(type):
    @classmethod
    def __prepare__(mcs, name, bases):
        return OrderedDict()

    def __new__(mcs, name, bases, attrs):
        cls = type.__new__(mcs, name, bases, attrs)
        cls.__limits__ = [
            (key[6:], func) for key, func in attrs.items()
            if key.startswith('limit_')
        ]
        for base in bases:
            cls.__limits__.extend(getattr(base, '__limits__', []))
        return cls


class BaseSelector(metaclass=SelectorMeta):
    """ The helper class that allows defining limits as methods.

    Extending classes can specify limits by declaring methods
    named ``limit_<runner name>`` that take one input parameters
    argument and return True or False whether this runner should
    be used. The methods are evaluated in order of declaration
    and the first one to return True is selected. Otherwise,
    the job is rejected.
    """
    def __call__(self, inputs):
        try:
            self.setup(inputs)
            return next(
                (name for name, func in self.__limits__ if func(self, inputs)),
                None
            )
        finally:
            self.__dict__.clear()

    def setup(self, inputs):
        pass

    @staticmethod
    def default(_inputs):
        return "default"
