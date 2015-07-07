# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import simcity
from picas import Job
from nose.tools import (assert_true, assert_equals, assert_raises,
                        assert_not_equals)
from test_mock import MockDB


class TestJob():

    def setup(self):
        self.db = MockDB()
        self.test_id = MockDB.JOBS[0]['_id']
        self.test_other_id = MockDB.JOBS[1]['_id']
        simcity.set_current_job_id(self.test_id)
        simcity.management._job_db = self.db

    def test_get_job(self):
        job = simcity.get_job()
        assert_equals(job.id, self.test_id)
        simcity.set_current_job_id(None)
        assert_raises(EnvironmentError, simcity.get_job)
        otherJob = simcity.get_job(self.test_other_id)
        assert_equals(otherJob.id, self.test_other_id)
        assert_equals(type(otherJob), type(Job({'_id': 'aaaa'})))

    def test_start_job(self):
        job = simcity.start_job()
        assert_equals(job.id, self.test_id)
        assert_true(job['start'] > 0)
        assert_true(simcity.get_job()['start'] > 0)
        assert_equals(job['queue'], 0)
        assert_equals(job['done'], 0)

    def test_finish_job(self):
        job = simcity.start_job()
        simcity.finish_job(job)
        assert_true(job['start'] > 0)
        assert_true(job['done'] > 0)
        assert_true(simcity.get_job()['done'] > 0)
        job = simcity.queue_job(job, 'ssh')
        assert_true(job['archive'] > 0)

    def test_queue_job(self):
        job = simcity.queue_job(Job({'_id': 'aaaa'}), 'ssh')
        assert_true(job['queue'] > 0)
        assert_equals(len(self.db.saved), 1)
        job = simcity.queue_job(simcity.get_job(), 'ssh')
        assert_true(job['queue'] > 0)
        assert_true(simcity.get_job()['queue'] > 0)
        job = simcity.finish_job(job)
        assert_true(job['archive'] > 0)

    def test_archive_job(self):
        job = simcity.get_job()
        self.db.save(job)
        job = simcity.archive_job(simcity.get_job())
        assert_not_equals(job.id, self.test_id)
