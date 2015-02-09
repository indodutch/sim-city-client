import unittest
from simcity_client.iterator import TokenViewIterator
from test_mock import MockDB

class TestTokenIter(unittest.TestCase):
    def testIterator(self):
        db = MockDB()
        for token in TokenViewIterator(db, 'view'):
            self.assertGreater(token['lock'], 0)
            self.assertEqual(token.rev, 'something')
            self.assertDictEqual(db.saved[token.id], token.value)
            break # process one token only
        
        self.assertEqual(len(db.saved), 1)