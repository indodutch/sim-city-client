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
