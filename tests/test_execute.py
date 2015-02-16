import unittest
from test_mock import MockDB, MockRun

class TestRun(unittest.TestCase):
    def callback(self, task):
        self.assertIn(task.id, [t['_id'] for t in MockDB.TASKS])
        self.assertGreater(task['lock'], 0)
        self.count += 1
        
    def testTask(self):
        self.count = 0
        runner = MockRun(self.callback)
        runner.run()
        self.assertEqual(self.count, len(MockDB.TASKS))

