import unittest
from test_mock import MockDB, MockRun

class TestRun(unittest.TestCase):
    def callback(self, token):
        self.assertIn(token.id, [t['_id'] for t in MockDB.TOKENS])
        self.assertGreater(token['lock'], 0)
        self.count += 1
        
    def testToken(self):
        self.count = 0
        runner = MockRun(self.callback)
        runner.run()
        self.assertEqual(self.count, len(MockDB.TOKENS))

