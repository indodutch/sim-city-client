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

import simcity
from simcity.task.document import Task
from simcity.iterator import ViewIterator
from couchdb.http import ResourceConflict


class TaskViewIterator(ViewIterator):

    """Iterator object to fetch tasks while available.
    """

    def __init__(self, view, database=None, **view_params):
        """
        @param client: CouchClient for handling the connection to the CouchDB
        server.
        @param database: CouchDB view from which to fetch the task.
        @param task_modifier: instance of a TaskModifier.
        @param view_params: parameters which need to be passed on to the view
        (optional).
        """
        simcity._check_init()
        if database is None:
            database = simcity.task.database
        super(TaskViewIterator, self).__init__(database, view, **view_params)

    def claim_task(self, allowed_failures=10):
        for _ in xrange(allowed_failures):
            try:
                doc = self.database.get_single_from_view(
                    self.view, window_size=100, **self.view_params)
                task = Task(doc)
                return self.database.save(task.lock())
            except ResourceConflict:
                pass

        raise EnvironmentError("Unable to claim task.")
