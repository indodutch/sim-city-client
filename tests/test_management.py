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


def test_init():
    pytest.raises(ValueError, simcity.init, 'thispathdoesnotexist.ini')
    cfg = simcity.Config()
    pytest.raises(KeyError, simcity.init, cfg)
    cfg.add_section('task-db', {
        'url': 'http://doesnotexistforsure_atleasti_think_so.nl/',
        'username': 'example',
        'password': 'example',
        'database': 'example',
    })
    pytest.raises(IOError, simcity.init, cfg)


def test_uses_webdav():
    simcity.management._config = simcity.Config()
    assert not simcity.uses_webdav()
    cfg = simcity.management._config
    webdav_cfg = {}
    cfg.add_section('webdav', webdav_cfg)
    assert not simcity.uses_webdav()
    webdav_cfg['url'] = 'something'
    assert simcity.uses_webdav()
    webdav_cfg['user'] = 'something'
    assert simcity.uses_webdav()
    webdav_cfg['enabled'] = False
    assert not simcity.uses_webdav()
    webdav_cfg['enabled'] = True
    assert simcity.uses_webdav()


def test_views(job_db, task_db):
    simcity.create_views()
    assert 'running_jobs' in job_db.views
    assert 'overview_total' in job_db.views
    assert 'pending' not in job_db.views

    assert 'pending' in task_db.views
    assert 'overview_total' in task_db.views
    assert 'running_jobs' not in task_db.views
