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

import simcity
from simcity.util import seconds
import os
import pytest


@pytest.mark.usefixtures('task_db')
def test_add_task():
    task = simcity.add_task({'key': 'my value'})
    assert task['key'] == 'my value'
    assert len(task.id) > 0


@pytest.mark.usefixtures('task_db')
def test_get_task(task_id):
    task = simcity.get_task(task_id)
    assert task.id == task_id


@pytest.mark.usefixtures('task_db')
def test_delete_task(dav, task_id):
    simcity.management._config = simcity.Config()
    assert 0 == len(dav.removed)
    task = simcity.get_task(task_id)
    dav.files['/myfile'] = {'url': 'ab'}
    task.files['myfile'] = {
        'url': dav.base_url + '/myfile'
    }
    simcity.delete_task(task)
    assert task_id not in simcity.get_task_database().tasks
    assert 2 == len(dav.removed)


def _upload_attachment(task_id, tmpdir, dav=None):
    task = simcity.get_task(task_id)
    f = tmpdir.join('tempfile.txt')
    f.write('ab')
    simcity.upload_attachment(task, f.dirname, f.basename)
    f.remove()

    if dav is not None:
        if len(task.id) >= 7:
            dav_path = task.id[5:7] + '/' + task.id + '/tempfile.txt'
        else:
            dav_path = task.id[:2] + '/' + task.id + '/tempfile.txt'
        return task, f.dirname, 'tempfile.txt', dav_path
    else:
        return task, f.dirname, 'tempfile.txt'


@pytest.mark.usefixtures('task_db')
def test_upload_attachment_couchdb(task_id, tmpdir):
    task, dirname, filename = _upload_attachment(task_id, tmpdir)

    assert '_attachments' in task
    assert filename in task.attachments
    assert filename not in task.files
    assert 'data' in task.attachments[filename]
    assert b'ab' == task.get_attachment(filename)['data']


@pytest.mark.usefixtures('task_db')
def test_upload_attachment_webdav(task_id, tmpdir, dav):
    task, dirname, filename, dav_path = _upload_attachment(task_id, tmpdir,
                                                           dav)

    assert '_attachments' not in task
    assert filename in task.files
    assert dav.base_url + '/' + dav_path == task.files[filename]['url']
    assert dav_path in dav.files, str(dav.files)
    assert b'ab' == dav.files[dav_path]


@pytest.mark.usefixtures('task_db')
def test_download_attachment_webdav(task_id, tmpdir, dav):
    task, dirname, filename, dav_path = _upload_attachment(task_id, tmpdir,
                                                           dav)

    path = dirname + '/' + filename
    assert not os.path.exists(path)
    simcity.download_attachment(task, dirname, filename)
    assert os.path.exists(path)
    with open(path, 'rb') as f:
        assert b'ab' == f.read()
    os.remove(path)


@pytest.mark.usefixtures('task_db')
def test_download_attachment_couchdb(task_id, tmpdir):
    task, dirname, filename = _upload_attachment(task_id, tmpdir)

    path = dirname + '/' + filename
    assert not os.path.exists(path)
    simcity.download_attachment(task, dirname, filename)
    assert os.path.exists(path)
    with open(path, 'rb') as f:
        assert b'ab' == f.read()
    os.remove(path)


@pytest.mark.usefixtures('task_db')
def test_delete_attachment_webdav(task_id, tmpdir, dav):
    task, dirname, filename, dav_path = _upload_attachment(task_id, tmpdir,
                                                           dav)

    assert dav_path in dav.files, str(dav.files)
    assert filename in task.files
    simcity.delete_attachment(task, filename)
    assert dav_path not in dav.files, str(dav.files)
    assert dav_path in dav.removed, str(dav.removed)
    assert filename not in task.files


@pytest.mark.usefixtures('task_db')
def test_delete_attachment_couchdb(task_id, tmpdir):
    task, dirname, filename = _upload_attachment(task_id, tmpdir)

    assert filename in task['_attachments']
    simcity.delete_attachment(task, filename)
    assert filename not in task['_attachments']


def test_scrub_task(task_db, task_id):
    task = simcity.get_task(task_id)
    assert 0 == task['lock']
    task.lock('myid')
    assert 0 != task['lock']
    task_db.tasks[task.id]['_rev'] = 'myrev'
    task_db.tasks[task.id]['lock'] = task['lock']
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    assert 0 == len(task_db.saved)

    simcity.scrub_tasks('in_progress', age=0)
    assert 1 == len(task_db.saved)
    task_id, task = task_db.saved.popitem()
    assert 0 == task['lock']


def test_scrub_old_task_none(task_db, task_id):
    task = simcity.get_task(task_id)
    task.lock('myid')
    assert 0 == len(task_db.saved)
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    simcity.scrub_tasks('in_progress', age=2)
    assert 0 == len(task_db.saved)


def test_scrub_old_task(task_db, task_id):
    task = simcity.get_task(task_id)
    task['lock'] = seconds() - 100
    assert 0 != task['lock']
    task_db.tasks[task.id]['_rev'] = 'myrev'
    task_db.tasks[task.id]['lock'] = task['lock']
    task_db.set_view([{'id': task.id, 'key': task.id, 'value': task}])
    assert 0 == len(task_db.saved)

    simcity.scrub_tasks('in_progress', age=2)
    assert 1 == len(task_db.saved)
    old_task_id = task.id
    task_id, task = task_db.saved.popitem()
    assert task_id == old_task_id
    assert 0 == task['lock']
