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

test_id = 'mydoc'
test_other_id = 'myotherdoc'


def test_create():
    doc = Document({'_id': test_id})
    assert doc.id == test_id
    assert doc == {'_id': test_id}
    doc['_id'] = test_other_id
    assert doc.id == test_other_id
    assert doc == {'_id': test_other_id}


def test_no_id():
    doc = Document({'someattr': 1})
    assert doc.id is None
    assert doc.rev is None


def test_empty():
    Document({})


def test_attachment():
    doc = Document()
    data = b"This is it"
    doc.put_attachment('mytext.txt', data)
    attach = doc.get_attachment('mytext.txt')
    assert attach['content_type'] == 'text/plain'
    assert attach['data'] == data
    assert doc['_attachments']['mytext.txt']['data'] == b'VGhpcyBpcyBpdA=='
    doc.delete_attachment('mytext.txt')
    assert 'mytext.txt' not in doc['_attachments']
    assert attach['data'] == data
    doc.put_attachment('mytext.json', b'{}')
    attach = doc.get_attachment('mytext.json')
    assert attach['content_type'] == 'application/json'


class TestTask:

    def setup(self):
        self.task = Task({'_id': test_id})

    def test_id(self):
        assert self.task.id == test_id
        assert self.task['_id'] == test_id

    def test_no_id(self):
        t = Task()
        assert len(t.id) > 10

    def test_done(self):
        assert self.task['done'] == 0
        self.task.done()
        assert self.task['done'] >= seconds() - 1

    def test_lock(self):
        assert self.task['lock'] == 0
        self.task.lock('myid')
        assert self.task['lock'] >= seconds() - 1
        assert 'myid' == self.task['job']

    def test_scrub(self):
        self.task.lock('myid')
        self.task.done()
        self.task.scrub()
        assert self.task['lock'] == 0
        assert self.task['done'] == 0
        assert self.task['scrub_count'] == 1
        self.task.scrub()
        assert self.task['lock'] == 0
        assert self.task['done'] == 0
        assert self.task['scrub_count'] == 2

    def test_error(self):
        self.task.error("some message")
        assert self.task['lock'] == -1
        assert self.task['done'] == -1
        self.task.scrub()
        assert self.task['lock'] == 0
        assert self.task['done'] == 0
        assert len(self.task['error']) == 1
