import pickle
import socket
import sys
import threading
import uuid

from collections import namedtuple

from .runnable_task import RunnableTask
from .utils import enum, Signal


class Job:
    def __init__(self, runnable, args=None, kwargs=None):
        """
        Initialize a new job which executes start method of the runnable
        as a separate thread.
        :param runnable: runnable started by the worker
        :param args: arguments passed to the runnable's start method
        :param kwargs: keyword arguments passed to the runnable's start method
        """
        if not isinstance(runnable, RunnableTask):
            raise TypeError("Runnable must implement RunnableTask")
        self.runnable = runnable
        self.id = uuid.uuid4().hex
        self.status = JobStatus.PENDING
        self.args = args or ()
        self.kwargs = kwargs or {}
        self._result = None
        self._exception = None
        self.sig_finished = Signal()

    def start(self):
        """
        Launches a new thread where the runnable is executed
        :return: id of the job
        """
        if not self.is_pending():
            raise RuntimeError("Job is already running or is completed")
        thread = threading.Thread(
            target=self._execute,
            args=self.args,
            kwargs=self.kwargs
        )
        thread.start()

    def _execute(self):
        """
        Executes the target function and waits for completion.
        """
        self.status = JobStatus.RUNNING
        try:
            self._result = self.runnable.run(*self.args, **self.kwargs)
        except RuntimeError:
            self._exception = sys.exc_info()
            self.status = JobStatus.FAILED
        else:
            self.status = JobStatus.COMPLETED
        finally:
            self.sig_finished()

    def kill(self):
        """
        Orders the running task to kill its process
        """
        self.runnable.kill()

    def suspend(self):
        """
        Tells the running task to suspend execution
        """
        self.runnable.suspend()

    def resume(self):
        """
        Tells the running task to resume suspended execution
        """
        self.runnable.resume()

    @property
    def result(self):
        """
        Returns job result if the job is finished, otherwise None
        :return: JobResult containing the output
        """
        if not self.is_finished():
            return None
        return JobResult(self._result, self._exception)

    def is_finished(self):
        """
        Checks if the job is finished
        """
        return (self.status == JobStatus.COMPLETED or
                self.status == JobStatus.FAILED)

    def is_pending(self):
        """
        Checks if the job is pending execution
        """
        return self.status == JobStatus.PENDING

    def is_running(self):
        """
        Checks if the job is already running
        """
        return self.status == JobStatus.RUNNING

    def __repr__(self):
        return "<Job> {id} - {status}".format(id=self.id, status=self.status)


JobStatus = enum(
    PENDING="PENDING",
    RUNNING="RUNNING",
    COMPLETED="COMPLETED",
    FAILED="FAILED"
)


JobResult = namedtuple("JobResult", ["result", "error"])
