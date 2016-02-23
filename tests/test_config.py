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

import tempfile
import os
from simcity.config import Config, FileConfig, CouchDBConfig
from test_mock import MockDB, MockRow

from nose.tools import (assert_true, assert_equals, assert_raises,
                        assert_dict_equal, assert_set_equal)


def test_config_write_read():
    fd, fname = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as f:
        print('[MySection]', file=f)
        print('a=4', file=f)
        print('[OtherSection]', file=f)
        print('a=2', file=f)
        print('b=wefa feaf', file=f)
        print('c=wefa=feaf', file=f)

    cfg = Config([FileConfig(fname)])
    os.remove(fname)

    my = cfg.section('MySection')
    other = cfg.section('OtherSection')
    assert_raises(KeyError, cfg.section,
                  'NonExistantSection')
    assert_true(type(my) is dict, "section is not a dictionary")
    assert_true('a' in my, "Value in section")
    assert_equals(
        my['a'], '4', "latest value does not overwrite earlier values")
    assert_equals(other['a'], '2', "value not contained to section")
    assert_equals(other['b'], 'wefa feaf', "spaces allowed")
    assert_equals(other['c'], 'wefa=feaf', "equals-sign allowed")
    sections = frozenset(['DEFAULT', 'MySection', 'OtherSection'])
    assert_set_equal(sections, cfg.sections())
    cfg.add_section('CustomSection', {})
    assert_set_equal(sections | frozenset(['CustomSection']), cfg.sections())


def test_empty_config():
    cfg = Config()
    assert_raises(KeyError, cfg.section, 'notexist')
    cfg.add_section('my', {'a': 'b'})
    assert_equals(cfg.section('my')['a'], 'b')


def test_nonexistant():
    assert_raises(ValueError, FileConfig, 'nonexistant.ini')


def test_couchconfig():
    db = MockDB([MockRow('', '', id='task-db'), MockRow('', '', id='job-db')])
    db.tasks = {
        'task-db':
            {'settings': {'url': 'http://task.example', 'name': 'tasks'}},
        'job-db':
            {'settings': {'url': 'http://job.example', 'name': 'jobs'}},
    }

    dbconfig = CouchDBConfig(db)
    assert_dict_equal({'url': 'http://task.example', 'name': 'tasks'},
                      dbconfig.section('task-db'))
    assert_set_equal(frozenset(['task-db', 'job-db']), dbconfig.sections())
    config = Config([dbconfig])
    config.add_section('my-section', {'url': 'that'})
    assert_set_equal(frozenset(['task-db', 'job-db', 'my-section']),
                     config.sections())
    assert_dict_equal({'url': 'that'}, config.section('my-section'))
    config.add_section('task-db', {'name': 'new-tasks'})
    assert_dict_equal({'url': 'http://task.example', 'name': 'new-tasks'},
                      config.section('task-db'))
