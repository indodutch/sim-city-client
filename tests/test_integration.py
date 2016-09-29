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
import pytest


def test_overview(db):
    db.set_view([('done', 1),
                 ('pending', 3),
                 ('finished_jobs', 2)])
    overview = simcity.overview_total()
    assert overview['done'] == 1
    assert overview['finished_jobs'] == 2
    assert overview['pending_jobs'] == 0


def test_run(task_db, job_db):
    job_db.set_view([('running_jobs', 1)])

    task, job = simcity.run_task({'key': 'value'}, 'myhost', 1)
    assert isinstance(task, simcity.Task)
    assert job is None
    assert task_db.get(task.id) is not None


def test_scrub_task(task_db, task_id):
    task = simcity.get_task(task_id)
    assert 0 == task['lock']
    task.lock('myid')
    assert 0 != task['lock']
    task_db.tasks[task.id]['_rev'] = 'myrev'
    task_db.tasks[task.id]['lock'] = task['lock']
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    assert 0 == len(task_db.saved)

    simcity.scrub('in_progress', age=0)
    assert 1 == len(task_db.saved)
    task_id, task = task_db.saved.popitem()
    assert 0 == task['lock']


def test_scrub_old_task_none(task_db, task_id):
    task = simcity.get_task(task_id)
    task.lock('myid')
    assert 0 == len(task_db.saved)
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    simcity.scrub('in_progress', age=2)
    assert 0 == len(task_db.saved)


def test_scrub_old_task(task_db, task_id):
    task = simcity.get_task(task_id)
    task['lock'] = simcity.util.seconds() - 100
    assert 0 != task['lock']
    task_db.tasks[task.id]['_rev'] = 'myrev'
    task_db.tasks[task.id]['lock'] = task['lock']
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    assert 0 == len(task_db.saved)

    simcity.scrub('in_progress', age=2)
    assert 1 == len(task_db.saved)
    old_task_id = task.id
    task_id, task = task_db.saved.popitem()
    assert task_id == old_task_id
    assert 0 == task['lock']


def test_scrub_job(job_db, job_id):
    job = simcity.get_job()
    job.start()
    assert 0 == job['archive']
    job_db.jobs[job.id]['_rev'] = 'myrev'
    job_db.jobs[job.id]['done'] = 0
    print(job_db.jobs[job.id])
    job_db.set_view([{'id': job.id, 'key': job.id, 'value': job}])
    assert 0 == len(job_db.saved)

    simcity.scrub('running_jobs', age=0)
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
    simcity.scrub('running_jobs', age=2)
    assert 0 == len(job_db.saved)


def test_scrub_old_job(job_db, job_id):
    job = simcity.get_job()
    job['start'] = simcity.util.seconds() - 100
    assert 0 == job['archive']
    job_db.jobs[job.id]['_rev'] = 'myrev'
    job_db.jobs[job.id]['done'] = 0
    job_db.set_view([{'id': job.id, 'key': job.id, 'value': job}])
    assert 0 == len(job_db.saved)

    simcity.scrub('running_jobs', age=2)
    assert 1 == len(job_db.saved)
    saved_id, job = job_db.saved.popitem()
    assert job['archive'] > 0
    assert saved_id == job_id
