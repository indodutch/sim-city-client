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
from simcity.document import Document
from simcity.task import Task
from simcity.util import seconds

class TestDocument(unittest.TestCase):
    def testCreate(self):
        _id = 'mydoc'
        other_id = 'myotherdoc'
        doc = Document({'_id': _id})
        self.assertEqual(doc.id, _id)
        self.assertEqual(doc.value, {'_id': _id})
        doc.id = other_id
        self.assertEqual(doc.id, other_id)
        self.assertEqual(doc.value, {'_id': other_id})
    
    def testNoId(self):
        doc = Document({'someattr': 1})
        self.assertRaises(AttributeError, getattr, doc, 'id')
        self.assertRaises(AttributeError, getattr, doc, 'rev')

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
        self.assertTrue(self.task['done'] >= seconds() - 1)

    def testLock(self):
        self.assertEqual(self.task['lock'], 0)
        self.task.lock()
        self.assertTrue(self.task['lock'] >= seconds() - 1)

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
        self.assertTrue(len(t.id) > 10)
