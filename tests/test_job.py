# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center
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
from simcity import Job
from simcity.util import seconds
import pytest


@pytest.mark.usefixtures('job_db')
def test_get_job(job_id, other_job_id):
    job = simcity.get_job()
    assert job.id == job_id
    simcity.set_current_job_id(None)
    pytest.raises(EnvironmentError, simcity.get_job)
    otherJob = simcity.get_job(other_job_id)
    assert otherJob.id == other_job_id
    assert type(otherJob) == type(Job({'_id': 'aaaa'}))  # noqa


@pytest.mark.usefixtures('job_db')
def test_start_job(job_id):
    job = simcity.start_job()
    assert job.id == job_id
    assert job['start'] > 0
    assert simcity.get_job()['start'] > 0
    assert job['queue'] == 0
    assert job['done'] == 0


@pytest.mark.usefixtures('job_id', 'job_db')
def test_finish_job():
    job = simcity.start_job()
    simcity.finish_job(job)
    assert job['start'] > 0
    assert job['done'] > 0
    assert simcity.get_job()['done'] > 0
    job = simcity.queue_job(job, 'ssh')
    assert job['archive'] > 0


def test_queue_job(job_db):
    job = simcity.queue_job(Job({'_id': 'aaaa'}), 'ssh')
    assert job['queue'] > 0
    assert len(job_db.saved) == 1
    job = simcity.queue_job(simcity.get_job(), 'ssh')
    assert job['queue'] > 0
    assert simcity.get_job()['queue'] > 0
    job = simcity.finish_job(job)
    assert job['archive'] > 0


def test_archive_job(job_id, job_db):
    job = simcity.get_job()
    job_db.save(job)
    job = simcity.archive_job(simcity.get_job())
    assert job.id == job_id


@pytest.mark.usefixtures('job_id', 'job_db')
def test_cancel_job():
    job = simcity.get_job()
    simcity.cancel_endless_job(job)
    assert job['cancel'] >= seconds() - 1
    assert job['cancel'] <= seconds()


def test_scrub_job(job_db, job_id):
    job = simcity.get_job()
    job.start()
    assert 0 == job['archive']
    job_db.jobs[job.id]['_rev'] = 'myrev'
    job_db.jobs[job.id]['done'] = 0
    print(job_db.jobs[job.id])
    job_db.set_view([{'id': job.id, 'key': job.id, 'value': job}])
    assert 0 == len(job_db.saved)

    simcity.scrub_jobs('running_jobs', age=0)
    assert 1 == len(job_db.saved)
    saved_id, job = job_db.saved.popitem()
    assert job['archive'] > 0
    assert saved_id == job_id


@pytest.mark.usefixtures('job_id')
def test_scrub_old_job_none(job_db):
    job = simcity.get_job()
    job.start()
    assert 0 == len(job_db.saved)
    job_db.set_view([{'id': job.id, 'key': job.id, 'value': job}])
    simcity.scrub_jobs('running_jobs', age=2)
    assert 0 == len(job_db.saved)


def test_scrub_old_job(job_db, job_id):
    job = simcity.get_job()
    job['start'] = seconds() - 100
    assert 0 == job['archive']
    job_db.jobs[job.id]['_rev'] = 'myrev'
    job_db.jobs[job.id]['done'] = 0
    job_db.set_view([{'id': job.id, 'key': job.id, 'value': job}])
    assert 0 == len(job_db.saved)

    simcity.scrub_jobs('running_jobs', age=2)
    assert 1 == len(job_db.saved)
    saved_id, job = job_db.saved.popitem()
    assert job['archive'] > 0
    assert saved_id == job_id
