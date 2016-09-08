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

""" Create and update tasks. """
from .document import Task


class TaskHandler(object):
    def __init__(self, database, attachment_handler):
        self.database = database
        self.attachment_handler = attachment_handler

    def add(self, properties):
        """
        Add a Task object to the database with given properties.
        """
        return self.database.save(Task(properties))

    def get(self, task_id):
        """
        Get the Task object with given ID.
        """
        return Task(self.database.get(task_id))

    def delete(self, task):
        """
        Delete a task and associated attachments from the database and webdav.
        """
        self.database.delete(task)

        for name in list(task.files.keys()):
            self.attachment_handler.delete(task, name)
