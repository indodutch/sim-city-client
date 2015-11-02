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
from nose.tools import assert_true, assert_raises
from test_mock import MockDB, MockDAV
import os
import shutil


def test_actor():
    simcity.management._reset_globals()
    try:
        shutil.rmtree('tests/tmp')
    except OSError:
        pass

    os.mkdir('tests/tmp')
    cfg = simcity.util.Config(from_file=False)
    cfg.add_section('Execution', {
        'tmp_dir': 'tests/tmp/tmp_alala',
        'output_dir': 'tests/tmp/out_alala',
        'input_dir': 'tests/tmp/in_alala',
    })
    cfg.add_section('webdav', {
        'url': 'https://my.example.com'
    })
    simcity.management._webdav[None] = MockDAV()
    db = MockDB()
    db.tasks = {'mytask': {'_id': 'mytask', 'command': 'echo'}}
    assert_raises(KeyError, simcity.management.set_config, cfg)
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    simcity.management.set_current_job_id('myjob')
    actor = simcity.ExecuteActor()
    actor.run()
    assert_true(db.jobs['myjob']['done'] > 0)
    assert_true(db.saved['mytask']['done'] > 0)
    assert_true(os.path.exists('tests/tmp/tmp_alala'))
    assert_true(os.path.exists('tests/tmp/out_alala'))
    assert_true(os.path.exists('tests/tmp/in_alala'))
    shutil.rmtree('tests/tmp')
