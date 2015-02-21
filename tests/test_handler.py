# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import mock

from rq import Worker
from tests import RQTestCase
from rq.job import Job
from rq.queue import Queue

try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps


def dome():
    return 100


def fail():
    raise ValueError("damn")


class NoopHandler(object):
    pass


class OnlyPreHandler(object):
    @staticmethod
    def will_perform(job):
        return 100
    
    
class OnlyPostHandler(object):
    @staticmethod
    def has_performed(job):
        return 100


class OnlyFailedHandler(object):
    @staticmethod
    def has_failed(job):
        return 100


class FullHandler(OnlyFailedHandler, OnlyPostHandler, OnlyPreHandler):
    pass


class ExceptionRaiseHandler(object):
    @staticmethod
    def will_perform(job):
        raise ValueError("oops!")

    @staticmethod
    def has_performed(job):
        raise ValueError("oops!")

    @staticmethod
    def has_failed(job):
        raise ValueError("oops!")


class TestJob(RQTestCase):
    def _setup(self, handler_cls, job_func=None):
        """Creation of new empty jobs."""
        handler = mock.Mock(spec=handler_cls)
        job = Job.create(func=job_func or dome, handler=handler)
        queue = Queue(async=False)
        worker = Worker([queue])
        worker.perform_job(job)
        self.assertTrue(job.created_at)
        return handler, job
    
    def test_will_only(self):
        handler, job = self._setup(OnlyPreHandler)
        handler.will_perform.assert_called_once_with(job)

    def test_post_only(self):
        handler, job = self._setup(OnlyPostHandler)
        handler.has_performed.assert_called_once_with(job)

    def test_handler_exception(self):
        handler, job = self._setup(ExceptionRaiseHandler, dome)
        handler.will_perform.assert_called_once_with(job)
        handler.has_performed.assert_called_once_with(job)

    def test_job_fail(self):
        handler, job = self._setup(FullHandler, fail)
        handler.will_perform.assert_called_once_with(job)
        self.assertEqual(handler.has_failed.call_count, 1)
        self.assertEqual(handler.has_failed.call_args[0][0], job)
        
    def test_noop(self):
        # we're making sure that a missing method won't havock anything
        handler, job = self._setup(NoopHandler)
        self.assertFalse(handler.called)
