import unittest
from simcity_client.document import Document, Token
from simcity_client.util import seconds

class TestDocument(unittest.TestCase):
    def testCreate(self):
        _id = 'mydoc'
        doc = Document({'_id': _id})
        self.assertEqual(doc.id, _id)
        self.assertDictEqual(doc.value, {'_id': _id})
    
    def testNoId(self):
        doc = Document({'someattr': 1})
        with self.assertRaises(KeyError):
            doc.id

    def testEmpty(self):
        Document({})

class TestToken(unittest.TestCase):
    def setUp(self):
        self._id = 'mydoc'
        self.token = Token({'_id': self._id})
    
    def testId(self):
        self.assertEqual(self.token.id, self._id)
        self.assertEqual(self.token.value['_id'], self._id)
        self.assertEqual(self.token['_id'], self._id)
    
    def testDone(self):
        self.assertEqual(self.token['done'], 0)
        self.token.mark_done()
        self.assertGreaterEqual(self.token['done'], seconds() - 1)
        self.token.unmark_done()
        self.assertEqual(self.token['done'], 0)

    def testLock(self):
        self.assertEqual(self.token['lock'], 0)
        self.token.lock()
        self.assertGreaterEqual(self.token['lock'], seconds() - 1)
        self.token.unlock()
        self.assertEqual(self.token['lock'], 0)

    def testScrub(self):
        self.token.lock()
        self.token.mark_done()
        self.token.scrub()
        self.assertEqual(self.token['lock'], 0)
        self.assertEqual(self.token['done'], 0)
        self.assertEqual(self.token['scrub_count'], 1)
        self.token.scrub()
        self.assertEqual(self.token['lock'], 0)
        self.assertEqual(self.token['done'], 0)
        self.assertEqual(self.token['scrub_count'], 2)
    
    def testEror(self):
        self.token.error("some message")
        self.assertEqual(self.token['lock'], -1)
        self.assertEqual(self.token['done'], -1)
        self.token.scrub()
        self.assertEqual(self.token['lock'], 0)
        self.assertEqual(self.token['done'], 0)
        self.assertEqual(len(self.token['error']), 1)