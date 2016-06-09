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

from simcity import Document, Task
from simcity.util import seconds
from nose.tools import assert_equals, assert_true

test_id = 'mydoc'
test_other_id = 'myotherdoc'


def test_create():
    doc = Document({'_id': test_id})
    assert_equals(doc.id, test_id)
    assert_equals(doc, {'_id': test_id})
    doc['_id'] = test_other_id
    assert_equals(doc.id, test_other_id)
    assert_equals(doc, {'_id': test_other_id})


def test_no_id():
    doc = Document({'someattr': 1})
    assert_equals(None, doc.id)
    assert_equals(None, doc.rev)


def test_empty():
    Document({})


def test_attachment():
    doc = Document()
    data = b"This is it"
    doc.put_attachment('mytext.txt', data)
    attach = doc.get_attachment('mytext.txt')
    assert_equals(attach['content_type'], 'text/plain')
    assert_equals(attach['data'], data)
    assert_equals(doc['_attachments']['mytext.txt']['data'],
                  b'VGhpcyBpcyBpdA==')
    doc.delete_attachment('mytext.txt')
    assert_true('mytext.txt' not in doc['_attachments'])
    assert_equals(attach['data'], data)
    doc.put_attachment('mytext.json', b'{}')
    attach = doc.get_attachment('mytext.json')
    assert_equals(attach['content_type'], 'application/json')


class TestTask:

    def setup(self):
        self.task = Task({'_id': test_id})

    def test_id(self):
        assert_equals(self.task.id, test_id)
        assert_equals(self.task['_id'], test_id)

    def test_no_id(self):
        t = Task()
        assert_true(len(t.id) > 10)

    def test_done(self):
        assert_equals(self.task['done'], 0)
        self.task.done()
        assert_true(self.task['done'] >= seconds() - 1)

    def test_lock(self):
        assert_equals(self.task['lock'], 0)
        self.task.lock()
        assert_true(self.task['lock'] >= seconds() - 1)

    def test_scrub(self):
        self.task.lock()
        self.task.done()
        self.task.scrub()
        assert_equals(self.task['lock'], 0)
        assert_equals(self.task['done'], 0)
        assert_equals(self.task['scrub_count'], 1)
        self.task.scrub()
        assert_equals(self.task['lock'], 0)
        assert_equals(self.task['done'], 0)
        assert_equals(self.task['scrub_count'], 2)

    def test_error(self):
        self.task.error("some message")
        assert_equals(self.task['lock'], -1)
        assert_equals(self.task['done'], -1)
        self.task.scrub()
        assert_equals(self.task['lock'], 0)
        assert_equals(self.task['done'], 0)
        assert_equals(len(self.task['error']), 1)
