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
import random

import multiprocessing

from simcity import Document


class MockRow(object):
    def __init__(self, key, value, id=None):
        self.id = id
        self.key = key
        self.value = value


class MockDAVConf(object):
    def __init__(self):
        self.hostname = 'https://my.example.com'
        self.root = ''


class MockDAV(object):
    def __init__(self, files=None):
        if files is None:
            files = {}
        self.files = files
        self.webdav = MockDAVConf()
        self.removed = []

    def upload_sync(self, remote_path, local_path):
        with open(local_path, 'rb') as f:
            self.files[remote_path] = f.read()

    def clean(self, path):
        try:
            del self.files[path]
            self.removed.append(path)
        except KeyError:
            print("Not removing missing directory")

    def mkdir(self, path):
        pass

    def download_sync(self, remote_path, local_path):
        print("file path: {0}".format(local_path))
        with open(local_path, 'wb') as f:
            f.write(self.files[remote_path])


class MockDB(object):
    TASKS = [{'_id': 'a', 'lock': 0}, {'_id': 'b', 'lock': 0}]
    JOBS = [{'_id': 'myjob'}, {'_id': 'myotherjob'}]

    def __init__(self, view=[]):
        self.manager = multiprocessing.Manager()
        self.tasks = dict((t['_id'], self.manager.dict(t.copy()))
                          for t in MockDB.TASKS)  # deep copy
        self.jobs = dict((t['_id'], self.manager.dict(t.copy()))
                         for t in MockDB.JOBS)
        self.saved = self.manager.dict({})
        self.viewList = self.manager.list(view)
        self.views = self.manager.dict({})

    def copy(self):
        return self

    def get_single_from_view(self, view, **view_params):
        idx = random.choice(list(self.tasks.keys()))
        return Document(self.tasks[idx])

    def get(self, idx):
        if idx in self.saved:
            doc = self.saved[idx]
        elif idx in self.tasks:
            doc = self.tasks[idx]
        elif idx in self.jobs:
            doc = self.jobs[idx]
        else:
            raise KeyError
        return Document(doc)

    def save(self, doc):
        try:
            doc['_rev'] = str(int(doc.rev) + 1)
        except:
            doc['_rev'] = '0'

        if doc.id in self.jobs:
            self.jobs[doc.id] = doc.value
        else:
            if doc.id in self.tasks:
                del self.tasks[doc.id]
            self.saved[doc.id] = doc.value

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
