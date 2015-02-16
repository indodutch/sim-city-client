import unittest
from simcity.task.iterator import TaskViewIterator
from test_mock import MockDB

class TestTaskIter(unittest.TestCase):
    def testIterator(self):
        db = MockDB()
        for task in TaskViewIterator('view', database=db):
            self.assertGreater(task['lock'], 0)
            self.assertEqual(task.rev, 'something')
            self.assertDictEqual(db.saved[task.id], task.value)
            break # process one task only
        
        self.assertEqual(len(db.saved), 1)