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

from simcity.iterator import TaskViewIterator
from test_mock import MockDB
from nose.tools import assert_equals, assert_true


def test_iterator():
    db = MockDB()
    for task in TaskViewIterator(db, 'view'):
        assert_true(task['lock'] > 0)
        assert_equals(task.rev, '0')
        assert_equals(db.saved[task.id], task.value)
        break  # process one task only

    assert_equals(len(db.saved), 1)
