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

import unittest
from test_mock import MockDB, MockRun

class TestRun(unittest.TestCase):
    def callback(self, task):
        self.assertTrue(task.id in [t['_id'] for t in MockDB.TASKS])
        self.assertTrue(task['lock'] > 0)
        self.count += 1
        
    def testTask(self):
        self.count = 0
        runner = MockRun(self.callback)
        runner.run()
        self.assertEqual(self.count, len(MockDB.TASKS))

