import unittest
from simcity.document import Document
from simcity.task import Task
from simcity.util import seconds

class TestDocument(unittest.TestCase):
    def testCreate(self):
        _id = 'mydoc'
        other_id = 'myotherdoc'
        doc = Document({'_id': _id})
        self.assertEqual(doc.id, _id)
        self.assertDictEqual(doc.value, {'_id': _id})
        doc.id = other_id
        self.assertEqual(doc.id, other_id)
        self.assertDictEqual(doc.value, {'_id': other_id})
    
    def testNoId(self):
        doc = Document({'someattr': 1})
        self.assertRaises(AttributeError, doc.getattr, 'id')
        self.assertRaises(AttributeError, doc.getattr, 'rev'):

    def testEmpty(self):
        Document({})
        
    def testAttachment(self):
        doc = Document()
        data = "This is it"
        doc.put_attachment('mytext.txt', data)
        attach = doc.get_attachment('mytext.txt')
        self.assertEqual(attach['content_type'], 'text/plain')
        self.assertEqual(attach['data'], data)
        self.assertEqual(doc['_attachments']['mytext.txt']['data'], "VGhpcyBpcyBpdA==")
        doc.remove_attachment('mytext.txt')
        self.assertTrue('mytext.txt' not in doc['_attachments'])
        self.assertEqual(attach['data'], data)
        doc.put_attachment('mytext.json', '{}')
        attach = doc.get_attachment('mytext.json')
        self.assertEqual(attach['content_type'], 'application/json')

class TestTask(unittest.TestCase):
    def setUp(self):
        self._id = 'mydoc'
        self.task = Task({'_id': self._id})
    
    def testId(self):
        self.assertEqual(self.task.id, self._id)
        self.assertEqual(self.task.value['_id'], self._id)
        self.assertEqual(self.task['_id'], self._id)
    
    def testDone(self):
        self.assertEqual(self.task['done'], 0)
        self.task.done()
        self.assertGreaterEqual(self.task['done'], seconds() - 1)

    def testLock(self):
        self.assertEqual(self.task['lock'], 0)
        self.task.lock()
        self.assertGreaterEqual(self.task['lock'], seconds() - 1)

    def testScrub(self):
        self.task.lock()
        self.task.done()
        self.task.scrub()
        self.assertEqual(self.task['lock'], 0)
        self.assertEqual(self.task['done'], 0)
        self.assertEqual(self.task['scrub_count'], 1)
        self.task.scrub()
        self.assertEqual(self.task['lock'], 0)
        self.assertEqual(self.task['done'], 0)
        self.assertEqual(self.task['scrub_count'], 2)
    
    def testEror(self):
        self.task.error("some message")
        self.assertEqual(self.task['lock'], -1)
        self.assertEqual(self.task['done'], -1)
        self.task.scrub()
        self.assertEqual(self.task['lock'], 0)
        self.assertEqual(self.task['done'], 0)
        self.assertEqual(len(self.task['error']), 1)
    
    def testNoId(self):
        t = Task()
        self.assertGreater(len(t.id), 10)