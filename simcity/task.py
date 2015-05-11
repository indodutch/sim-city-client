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

from picas.documents import Task
from .management import get_task_database
import time


def add_task(properties, database=None):
    """
    Add a picas.documents.Task object to the database with given properties.
    """
    if database is None:
        database = get_task_database()

    t = Task(properties)
    return database.save(t)


def get_task(task_id, database=None):
    """
    Get the picas.documents.Task object with given ID.
    """
    if database is None:
        database = get_task_database()

    return Task(database.get(task_id))


def scrub_tasks(view, age=24*60*60, database=None):
    views = ['locked', 'error']
    if view not in views:
        raise ValueError('View "%s" not one of "%s"' % (view, str(views)))

    if database is None:
        database = get_task_database()

    min_t = int(time.time()) - age
    total = 0
    updates = []
    for row in database.view(view):
        total += 1
        if age <= 0 or row.value['lock'] < min_t:
            task = get_task(row.id)
            updates.append(task.scrub())

    if len(updates) > 0:
        database.save_documents(updates)

    return (len(updates), total)
