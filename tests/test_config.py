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

from simcity.config import Config, FileConfig, CouchDBConfig
import pytest


def test_config_write_read(tmpdir):
    f = tmpdir.join('test.ini')
    f.write('[MySection]\n'
            'a=4\n'
            '[OtherSection]\n'
            'a=2\n'
            'b=wefa feaf\n'
            'c=wefa=feaf\n')

    cfg = Config([FileConfig(str(f))])

    my = cfg.section('MySection')
    other = cfg.section('OtherSection')
    pytest.raises(KeyError, cfg.section, 'NonExistantSection')
    assert type(my) is dict, "section is not a dictionary"
    assert 'a' in my, "Value in section"
    assert my['a'] == '4', "latest value does not overwrite earlier values"
    assert other['a'] == '2', "value not contained to section"
    assert other['b'] == 'wefa feaf', "spaces allowed"
    assert other['c'] == 'wefa=feaf', "equals-sign allowed"
    sections = frozenset(['DEFAULT', 'MySection', 'OtherSection'])
    assert sections == cfg.sections()
    cfg.add_section('CustomSection', {})
    assert sections | frozenset(['CustomSection']) == cfg.sections()


def test_empty_config():
    cfg = Config()
    pytest.raises(KeyError, cfg.section, 'notexist')
    cfg.add_section('my', {'a': 'b'})
    assert cfg.section('my')['a'] == 'b'


def test_nonexistant():
    pytest.raises(ValueError, FileConfig, 'nonexistant.ini')


def test_couchconfig(db):
    db.set_view([{'id': 'task-db'}, {'id': 'job-db'}])
    db.tasks = {
        'task-db':
            {'settings': {'url': 'http://task.example', 'name': 'tasks'}},
        'job-db':
            {'settings': {'url': 'http://job.example', 'name': 'jobs'}},
    }

    dbconfig = CouchDBConfig(db)
    assert ({'url': 'http://task.example', 'name': 'tasks'} ==
            dbconfig.section('task-db'))
    assert frozenset(['task-db', 'job-db']) == dbconfig.sections()
    config = Config([dbconfig])
    config.add_section('my-section', {'url': 'that'})
    assert frozenset(['task-db', 'job-db', 'my-section']) == config.sections()
    assert {'url': 'that'} == config.section('my-section')
    config.add_section('task-db', {'name': 'new-tasks'})
    assert ({'url': 'http://task.example', 'name': 'new-tasks'} ==
            config.section('task-db'))
