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
import picas
from nose.tools import assert_equals, assert_true
from test_mock import MockDB, MockRow


def test_overview():
    simcity.management._reset_globals()
    db = MockDB(view=[MockRow('done', 1),
                      MockRow('todo', 3),
                      MockRow('finished_jobs', 2)])
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    overview = simcity.overview_total()
    assert_equals(overview['done'], 1)
    assert_equals(overview['finished_jobs'], 2)
    assert_equals(overview['pending_jobs'], 0)


def test_run():
    simcity.management._reset_globals()
    task_db = MockDB()
    simcity.management.set_task_database(task_db)
    job_db = MockDB(view=[MockRow('active_jobs', 1)])
    simcity.management.set_job_database(job_db)

    task, job = simcity.run_task({'key': 'value'}, 'myhost', 1)
    assert_true(isinstance(task, picas.Task))
    assert_true(job is None)
    assert_true(task_db.get(task.id) is not None)
