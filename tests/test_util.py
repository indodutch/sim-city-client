# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from simcity.util import (expandfilenames, issequence, Timer, parse_parameters,
                          expandfilename, get_truthy, seconds_to_str, seconds,
                          sizeof_fmt, copyglob, is_geojson, data_content_type,
                          file_content_type, listfiles, listdirs)
import os
import pytest
import time


def test_seq():
    assert issequence(())
    assert issequence(('a', 'b'))
    assert issequence([])
    assert issequence(['a'])
    assert not issequence('')
    assert not issequence('a')
    assert not issequence(1)
    assert not issequence({})
    assert not issequence(set())


def test_paths():
    value = expandfilenames(
        ['config.ini', ['~', 'home'], ('..', 'config.ini')])
    expected = ['config.ini', os.path.expanduser('~/home'), '../config.ini']
    assert value == expected

    assert expandfilenames('config.ini') == ['config.ini']
    assert expandfilenames([]) == []


def test_path():
    assert expandfilename('config.ini') == 'config.ini'
    assert expandfilename(['~', 'home']) == os.path.expanduser('~/home')


@pytest.mark.parametrize('test_input,expected', [
    (True, True),
    (1, True),
    (2, True),
    ('1', True),
    ('yes', True),
    ('true', True),
    ('on', True),
    (False, False),
    (0, False),
    ('any other string', False),
])
def test_truthy(test_input, expected):
    assert expected == get_truthy(test_input)


def test_seconds():
    assert seconds() > 0
    assert time.time() - 1 <= seconds() <= time.time()


@pytest.mark.parametrize('timestamp,default_value,expected', [
    (-1, 'nothing', 'nothing'),
    (-1, None, None),
    (-1, 1, 1),
    (0, 'nothing', 'nothing'),
])
def test_seconds_str_default(timestamp, default_value, expected):
    assert seconds_to_str(timestamp, default_value=default_value) == expected


def test_seconds_str():
    # actual hour will be different if the test system is in another location
    result = seconds_to_str(1)
    assert result.startswith('1970-01-01 ')
    assert result.endswith(':00:01')
    result = seconds_to_str(1471953832)
    assert result.startswith('2016-08-23 ')
    assert result.endswith(':03:52')


@pytest.mark.parametrize('length,byte_str', [
    (0, '0 B'),
    (1, '1 B'),
    (100, '100 B'),
    (1000, '1.0 kB'),
    (10000, '10.0 kB'),
    (1000000, '1.0 MB'),
    (1000000000, '1.0 GB'),
    (1550000000, '1.6 GB'),
    (1550000000000000000000000, '1.6 YB'),
    (15500000000000000000000000000, '15500.0 YB'),
    (-1, '-1 B'),
    (-100, '-100 B'),
    (-1000, '-1.0 kB'),
])
def test_size_fmt(length, byte_str):
    assert sizeof_fmt(length) == byte_str
    assert sizeof_fmt(float(length)) == byte_str


def test_timer():
    timer = Timer()
    time.sleep(0.2)
    assert timer.elapsed() >= 0.2
    assert timer.elapsed() < 0.4
    timer.reset()
    assert timer.elapsed() < 0.2


@pytest.fixture
def point2d_ref():
    return {'$ref': 'https://simcity.amsterdam-complexity.nl/schema/point2d'}


def test_parse_parameters(point2d_ref):
    parameters = {
        'a': 'bla',
        'b': 1,
        'c': {
            'x': 0.5,
            'y': 3,
            'name': 'lala'
        },
        'd': 'ja',
        'e': [
            'za',
            'na'
        ],
        'f': [
            {'x': 1, 'y': 2},
            {'x': -1, 'y': 1},
        ]
    }
    parameter_specs = {
        'properties': {
            'a': {'type': 'string'},
            'b': {'type': 'number'},
            'c': {
                'allOf': [
                   point2d_ref,
                   {'properties': {'name': {'type': 'string'}}}
                ]
            },
            'd': {'enum': ['ja', 'da']},
            'e': {'items': {'type': 'string'},
                  'minItems': 1,  'maxItems': 2},
            'f': {'items': point2d_ref},
        },
    }
    parse_parameters(parameters, parameter_specs)
    assert 1 == parameters['b']
    assert 0.5 == parameters['c']['x']


def test_missing_parameter():
    parameters = {}
    parameter_specs = {
        'properties': {'a': {'type': 'string'}},
        'required': ['a']
    }
    pytest.raises(ValueError, parse_parameters, parameters, parameter_specs)


def test_wrongtype_parameter():
    parameters = {'a': 'bla'}
    parameter_specs = {'properties': {
        'a': {'type': 'number'},
    }}
    pytest.raises(ValueError, parse_parameters, parameters, parameter_specs)


def test_wrong_maxlen():
    parameters = {
        'a': 'bla'
    }
    parameter_specs = {'properties': {
        'a': {'type': 'string', 'maxLength': 2}
    }}
    pytest.raises(ValueError, parse_parameters, parameters, parameter_specs)


def test_wrong_minlen():
    parameters = {
        'a': 'bla'
    }
    parameter_specs = {'properties': {
        'a': {'type': 'string', 'minLength': 4}
    }}
    pytest.raises(ValueError, parse_parameters, parameters, parameter_specs)


def test_simple_point(point2d_ref):
    parameters = {'a': {'x': 1, 'y': 2}}
    parameter_specs = {'properties': {'a': point2d_ref}}
    parse_parameters(parameters, parameter_specs)


def test_faulty_spec():
    parameters = {}
    parameter_specs = {'type': 'nonexistant'}
    pytest.raises(EnvironmentError, parse_parameters, parameters,
                  parameter_specs)


def test_extended_point(point2d_ref):
    parameters = {'a': {
        'x': 1,
        'y': 2,
        'name': 'mine',
        'id': 'la',
    }}
    parameter_specs = {'properties': {
        'a': {
            'allOf': [
                {'properties': {'name': {'type': 'string'}}},
                point2d_ref
            ]
        }
    }}
    parse_parameters(parameters, parameter_specs)


def test_copyglob(tmpdir):
    dir1 = tmpdir.mkdir('dir1')
    dir1.ensure('test.txt')
    dir1.ensure('test.csv')

    dir2 = tmpdir.mkdir('dir2')
    result = copyglob(str(dir1) + '/*.txt', str(dir2))
    assert result == [str(dir2.join('test.txt'))]
    assert dir2.join('test.txt').exists()
    assert not dir2.join('test.csv').exists()

    dir3 = tmpdir.mkdir('dir3')
    result = copyglob(str(dir1) + '/*.csv', str(dir3), prefix='abra_')
    assert result == [str(dir3.join('abra_test.csv'))]
    assert not dir3.join('test.txt').exists()
    assert not dir3.join('abra_test.txt').exists()
    assert not dir3.join('test.csv').exists()
    assert dir3.join('abra_test.csv').exists()

    dir4 = tmpdir.mkdir('dir4')
    result = copyglob(str(dir1) + '/*.json', str(dir4), prefix='abra_')
    assert result == []
    assert len(dir4.listdir()) == 0

    pytest.raises(ValueError, copyglob, str(dir1) + '/*.csv',
                  str(dir1.ensure('some_file.geo')))


def test_is_geojson(tmpdir):
    """ Whether given file pointer contains GeoJSON data. """
    f = tmpdir.join('geo.json')
    f.write('{"type":"Feature"}')
    assert is_geojson(f.open('r'))
    assert is_geojson(f.open('rb'))
    f.write('{"type":"bla"}')
    assert not is_geojson(f.open('r'))
    assert not is_geojson(f.open('rb'))
    f.write('{}')
    assert not is_geojson(f.open())
    f.write('some text')
    assert not is_geojson(f.open())


@pytest.mark.parametrize('filename,content,content_type', [
    ('geo.json', b'', 'application/json'),
    ('some.txt', b'', 'text/plain'),
    ('some.html', b'', 'text/html'),
    ('some.nonexistant', b'', 'text/plain'),
    ('geo.json', b'{"type":"Feature"}', 'application/vnd.geo+json'),
    ('geo.txt', b'{"type":"Feature"}', 'text/plain'),
])
def test_data_content_type(filename, content, content_type):
    assert data_content_type(filename, content) == content_type


@pytest.mark.parametrize('filename,content,content_type', [
    ('geo.json', b'', 'application/json'),
    ('some.txt', b'', 'text/plain'),
    ('some.html', b'', 'text/html'),
    ('some.nonexistant', b'', 'text/plain'),
    ('geo.json', b'{"type":"Feature"}', 'application/vnd.geo+json'),
    ('geo.txt', b'{"type":"Feature"}', 'text/plain'),
])
def test_file_content_type(tmpdir, filename, content, content_type):
    f = tmpdir.join(filename)
    f.write(content, mode='wb')
    assert file_content_type(filename, str(f)) == content_type


def test_listfiles(tmpdir):
    tmpdir.ensure('a.txt')
    tmpdir.ensure('b.txt')
    dir1 = tmpdir.mkdir('dir1')
    dir2 = tmpdir.mkdir('dir2')
    dir1.ensure('d.txt')
    dir2.mkdir('dir3')
    assert sorted(listfiles(str(tmpdir))) == ['a.txt', 'b.txt']
    assert sorted(listdirs(str(tmpdir))) == ['dir1', 'dir2']
    pytest.raises(OSError, listfiles, 'non_existant_dir')
    pytest.raises(OSError, listdirs, 'non_existant_dir')
