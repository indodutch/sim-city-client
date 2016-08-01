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
from nose.tools import assert_true, assert_raises, assert_equals
from test_mock import MockDB, MockDAV, setup_mock_directories
import os
import time


def test_actor():
    simcity.management._reset_globals()

    cfg = simcity.Config()
    exec_config = {'parallelism': 1}
    exec_config.update(setup_mock_directories())
    cfg.add_section('Execution', exec_config)
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
    iterator = simcity.TaskViewIterator('myid', db, 'todo')
    actor = simcity.JobActor(iterator, simcity.ExecuteWorker)
    actor.run()
    assert_true(db.jobs['myjob']['done'] > 0)
    assert_true(db.saved['mytask']['done'] > 0)
    assert_true(os.path.exists(exec_config['tmp_dir']))
    assert_true(os.path.exists(exec_config['output_dir']))
    assert_true(os.path.exists(exec_config['input_dir']))


def test_actor_maximize_parallelism():
    simcity.management._reset_globals()

    cfg = simcity.Config()
    exec_config = {'parallelism': 1}
    exec_config.update(setup_mock_directories())
    cfg.add_section('Execution', exec_config)
    cfg.add_section('webdav', {
        'url': 'https://my.example.com'
    })
    simcity.management._webdav[None] = MockDAV()
    db = MockDB()
    db.tasks = {'mytask': {'_id': 'mytask', 'command': 'echo',
                           'parallelism': '2'}}
    assert_raises(KeyError, simcity.management.set_config, cfg)
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    simcity.management.set_current_job_id('myjob')
    iterator = simcity.TaskViewIterator('myjob', db, 'todo')
    actor = simcity.JobActor(iterator, simcity.ExecuteWorker)
    actor.run()
    assert_true(db.jobs['myjob']['start'] > 0)
    assert_equals('myjob', db.saved['mytask']['job'])
    assert_true(db.jobs['myjob']['done'] > 0)
    assert_true(db.saved['mytask']['done'] > 0)
    assert_true(os.path.exists(exec_config['tmp_dir']))
    assert_true(os.path.exists(exec_config['output_dir']))
    assert_true(os.path.exists(exec_config['input_dir']))


def test_actor_parallelism():
    simcity.management._reset_globals()

    cfg = simcity.Config()
    exec_config = {'parallelism': 2}
    exec_config.update(setup_mock_directories())
    cfg.add_section('Execution', exec_config)
    cfg.add_section('webdav', {
        'url': 'https://my.example.com'
    })
    simcity.management._webdav[None] = MockDAV()
    db = MockDB()
    db.tasks = {'mytask': {'_id': 'mytask', 'command': 'sleep',
                           'arguments': ['0.3']},
                'mytask2': {'_id': 'mytask2', 'command': 'sleep',
                            'arguments': ['0.3']}}
    assert_raises(KeyError, simcity.management.set_config, cfg)
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    simcity.management.set_current_job_id('myjob')
    iterator = simcity.TaskViewIterator('myid', db, 'todo')
    actor = simcity.JobActor(iterator, simcity.ExecuteWorker)
    t0 = time.time()
    actor.run()
    print(db.jobs)
    print(db.saved)
    elapsed = time.time() - t0
    assert_true(elapsed < 0.6)
    assert_true(elapsed > 0.3)
    assert_true(db.jobs['myjob']['done'] > 0)
    assert_true(db.saved['mytask']['done'] > 0)
    assert_true(os.path.exists(exec_config['tmp_dir']))
    assert_true(os.path.exists(exec_config['output_dir']))
    assert_true(os.path.exists(exec_config['input_dir']))
