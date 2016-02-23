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
from simcity.config import Config, FileConfig

from nose.tools import assert_true, assert_equals, assert_raises


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
    sections = {'DEFAULT', 'MySection', 'OtherSection'}
    assert_equals(sections, cfg.sections())
    cfg.add_section('CustomSection', {})
    assert_equals(sections | {'CustomSection'}, cfg.sections())


def test_empty_config():
    cfg = Config()
    assert_raises(KeyError, cfg.section, 'notexist')
    cfg.add_section('my', {'a': 'b'})
    assert_equals(cfg.section('my')['a'], 'b')


def test_nonexistant():
    assert_raises(ValueError, FileConfig, 'nonexistant.ini')
