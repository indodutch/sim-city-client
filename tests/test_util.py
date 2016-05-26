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

from simcity.util import (expandfilenames, issequence, merge_dicts, Timer,
                          expandfilename, get_truthy)
import os
from nose.tools import assert_true, assert_false, assert_equals
import time


def test_seq():
    assert_true(issequence(()))
    assert_true(issequence(("a", "b")))
    assert_true(issequence([]))
    assert_true(issequence(["a"]))
    assert_false(issequence(""))
    assert_false(issequence("a"))
    assert_false(issequence(1))
    assert_false(issequence({}))
    assert_false(issequence(set()))


def test_paths():
    value = expandfilenames(
        ['config.ini', ['~', 'home'], ('..', 'config.ini')])
    expected = [
        'config.ini', os.path.expanduser('~/home'), '../config.ini']
    assert_equals(value, expected)

    assert_equals(expandfilenames('config.ini'), ['config.ini'])
    assert_equals(expandfilenames([]), [])


def test_path():
    assert_equals(expandfilename('config.ini'), 'config.ini')
    assert_equals(
        expandfilename(['~', 'home']), os.path.expanduser('~/home'))


def test_truthy():
    assert_true(get_truthy(True))
    assert_true(get_truthy(1))
    assert_true(get_truthy(2))
    assert_true(get_truthy("1"))
    assert_true(get_truthy("yes"))
    assert_true(get_truthy("true"))
    assert_true(get_truthy("on"))
    assert_false(get_truthy(False))
    assert_false(get_truthy(0))
    assert_false(get_truthy("anything else"))


class TestMerge():

    def setup(self):
        self.a = {'a': 1, 'b': 2}
        self.b = {'a': 2, 'c': 3}

    def test_merge_all(self):
        c = merge_dicts(self.a, self.b)
        assert_equals(c['a'], self.b['a'])
        assert_equals(self.b['a'], 2)
        assert_equals(self.a['a'], 1)
        assert_equals(len(self.a), 2)
        assert_equals(len(self.b), 2)
        assert_equals(len(c), 3)
        assert_equals(c['b'], self.a['b'])
        assert_equals(c['c'], self.b['c'])

    def test_merge_empty(self):
        c = merge_dicts(self.a, {})
        assert_equals(c, self.a)

    def test_empty_merge(self):
        c = merge_dicts({}, self.a)
        assert_equals(c, self.a)

    def test_empty_empty_merge(self):
        assert_equals(merge_dicts({}, {}), {})


def test_timer():
    timer = Timer()
    time.sleep(0.2)
    assert_true(timer.elapsed() >= 0.2)
    assert_true(timer.elapsed() < 0.4)
    timer.reset()
    assert_true(timer.elapsed() < 0.2)
