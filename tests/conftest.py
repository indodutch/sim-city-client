#!/usr/bin/env python
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
import argparse
import random
import multiprocessing
import pytest
import simcity


class MockRow(object):
    def __init__(self):
        self.id = None
        self.key = ''
        self.value = ''


class MockDAV(object):
    def __init__(self):
        self.files = {}
        self.base_url = 'https://my.example.com'
        self.removed = []

    def put(self, path, fp, content_type=None, content_length=None):
        self.files[path] = fp.read()

    def delete(self, path, ignore_not_existing=False):
        try:
            del self.files[path]
        except KeyError:
            if ignore_not_existing:
                print("Not removing missing file or directory")
            else:
                raise IOError()

        self.removed.append(path)

    def mkdir(self, path, ignore_existing=False):
        self.files[path] = 'DIR'

    def download(self, path, file_path):
        with open(file_path, 'wb') as f:
            f.write(self.files[path])

    def path_to_url(self, path):
        return self.base_url + '/' + path.lstrip('/')

    def url_to_path(self, url):
        if not url.startswith(self.base_url):
            raise ValueError()
        return url[len(self.base_url):].lstrip('/')


@pytest.fixture
def dav():
    return MockDAV()


class MockDB(object):
    def __init__(self):
        self.manager = multiprocessing.Manager()

        task_list = [{'_id': 'a', 'lock': 0}, {'_id': 'b', 'lock': 0}]
        job_list = [{'_id': 'myjob'}, {'_id': 'myotherjob'}]

        self.tasks = dict((t['_id'], self.manager.dict(t))
                          for t in task_list)
        self.jobs = dict((j['_id'], self.manager.dict(j))
                         for j in job_list)
        self.saved = self.manager.dict({})
        self.viewList = []
        self.views = self.manager.dict({})

    def set_view(self, view):
        self.viewList = []
        for row in view:
            mock_row = MockRow()
            if isinstance(row, tuple):
                mock_row.key, mock_row.value = row
            else:
                if 'id' in row:
                    mock_row.id = row['id']
                if 'key' in row:
                    mock_row.key = row['key']
                if 'value' in row:
                    mock_row.value = row['value']
            self.viewList.append(mock_row)

    def copy(self):
        return self

    def get_single_from_view(self, view, **view_params):
        idx = random.choice(list(self.tasks.keys()))
        return simcity.Document(self.tasks[idx])

    def get(self, idx):
        if idx in self.saved:
            doc = self.saved[idx]
        elif idx in self.tasks:
            doc = self.tasks[idx]
        elif idx in self.jobs:
            doc = self.jobs[idx]
        else:
            raise ValueError
        return simcity.Document(doc)

    def save(self, doc):
        try:
            doc['_rev'] = str(int(doc.rev) + 1)
        except:
            doc['_rev'] = '0'

        if doc.id in self.jobs:
            del self.jobs[doc.id]
        elif doc.id in self.tasks:
            del self.tasks[doc.id]
        self.saved[doc.id] = doc

        return doc

    def save_documents(self, docs):
        for doc in docs:
            self.save(doc)

    def delete(self, doc):
        if doc.id in self.jobs:
            del self.jobs[doc.id]
        elif doc.id in self.tasks:
            del self.tasks[doc.id]
        elif doc.id in self.saved:
            del self.saved[doc.id]

    def view(self, name, **view_options):
        return self.viewList

    def set_users(self, admins=None, members=None, admin_roles=None,
                  member_roles=None):
        pass

    def add_view(self, view, map_fun, reduce_fun=None, design_doc="Monitor",
                 *args, **kwargs):
        self.views[view] = {
            'map': map_fun,
            'reduce': reduce_fun,
            'design': design_doc
        }


@pytest.fixture
def job_db():
    return MockDB()


@pytest.fixture
def task_db():
    return MockDB()


@pytest.fixture
def db():
    return MockDB()


@pytest.fixture
def job_id():
    return 'myjob'


@pytest.fixture
def other_job_id():
    return 'myotherjob'


@pytest.fixture
def task_id():
    return 'a'


@pytest.fixture
def mock_directories(tmpdir):
    return {
        'tmp_dir': str(tmpdir.mkdir('tmp_alala')),
        'output_dir': str(tmpdir.mkdir('out_alala')),
        'input_dir': str(tmpdir.mkdir('in_alala')),
    }


class MockArgumentParser(argparse.ArgumentParser):
    def error(self, *args, **kwargs):
        # just return parsed arguments, no matter what
        pass


@pytest.fixture
def argument_parser():
    return MockArgumentParser()
