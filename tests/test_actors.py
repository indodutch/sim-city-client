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
from nose.tools import assert_true, assert_raises
from test_mock import MockDB
import os
import shutil


def test_actor():
    cfg = simcity.util.Config(from_file=False)
    cfg.add_section('Execution', {
        'tmp_dir': '$TMPDIR/tmp_alala',
        'output_dir': '$TMPDIR/out_alala',
        'input_dir': '$TMPDIR/in_alala',
    })
    db = MockDB()
    db.tasks = {'mytask': {'_id': 'mytask', 'command': 'echo'}}
    assert_raises(EnvironmentError, simcity.management.set_config, cfg)
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    simcity.management.set_current_job_id('myjob')
    actor = simcity.ExecuteActor()
    actor.run()
    assert_true(db.jobs['myjob']['done'] > 0)
    assert_true(db.saved['mytask']['done'] > 0)
    assert_true(os.path.exists(os.path.expandvars('$TMPDIR/tmp_alala')))
    assert_true(os.path.exists(os.path.expandvars('$TMPDIR/out_alala')))
    assert_true(os.path.exists(os.path.expandvars('$TMPDIR/in_alala')))
    shutil.rmtree(os.path.expandvars('$TMPDIR/tmp_alala'))
    shutil.rmtree(os.path.expandvars('$TMPDIR/out_alala'))
    shutil.rmtree(os.path.expandvars('$TMPDIR/in_alala'))
