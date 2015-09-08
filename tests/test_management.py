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
from test_mock import MockDB
from nose.tools import assert_raises, assert_in, assert_not_in


def test_init():
    simcity.management._reset_globals()
    assert_raises(ValueError, simcity.init, 'thispathdoesnotexist.ini')
    cfg = simcity.Config(from_file=False)
    assert_raises(KeyError, simcity.init, cfg)
    cfg.add_section('task-db', {
        'url': 'http://doesnotexistforsure_atleasti_think_so.nl/',
        'username': 'example',
        'password': 'example',
        'database': 'example',
    })
    assert_raises(IOError, simcity.init, cfg)


def test_views():
    simcity.management._reset_globals()
    simcity.management._job_db = MockDB()
    simcity.management._task_db = MockDB()
    simcity.create_views()
    assert_in('active_jobs',      simcity.management._job_db.views)
    assert_in('overview_total',   simcity.management._job_db.views)
    assert_not_in('todo',     simcity.management._job_db.views)

    assert_in('todo',             simcity.management._task_db.views)
    assert_in('overview_total',   simcity.management._task_db.views)
    assert_not_in('active_jobs', simcity.management._task_db.views)
