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
from .management import get_task_database, get_webdav
import time
import os


def add_task(properties, database=None):
    """
    Add a Task object to the database with given properties.
    """
    if database is None:
        database = get_task_database()

    t = Task(properties)
    return database.save(t)


def get_task(task_id, database=None):
    """
    Get the Task object with given ID.
    """
    if database is None:
        database = get_task_database()

    return Task(database.get(task_id))


def delete_task(task, database=None):
    """
    Delete a task and associated attachments from the database and webdav.
    """
    if database is None:
        database = get_task_database()

    database.delete(task)

    if len(task.uploads) > 0:
        # Not a for loop, since task.uploads is modified in delete_attachment.
        while len(task.uploads) > 0:
            filename = next(iter(task.uploads.keys()))
            delete_attachment(task, filename)

        dav = get_webdav()
        task_dir = _webdav_id_to_path(task.id)[1]
        if dav.exists(task_dir):
            dav.rmdir(task_dir)


def scrub_tasks(view, age=24 * 60 * 60, database=None):
    """
    Intends to update task metadata of defunct tasks.

    The tasks in given view will returned to jobs to be processed
    if their starting time is before given age.

    Parameters
    ----------
    view : one of (locked, error)
        View to scrub tasks from
    age : int
        select tasks started at least this number of seconds ago. Set to at
        most 0 to select all tasks.
    database : couchdb database, optional
        database to update the tasks from. Defaults to
        simcity.get_task_database()

    Returns
    -------
    A tuple with (the number of documents updated,
                  total number of documents in given view)
    """
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


def upload_attachment(task, directory, filename, mimetype=None):
    """ Uploads an attachment using the configured file storage layer. """
    with open(os.path.join(directory, filename), 'rb') as f:
        try:
            dav = get_webdav()
            path, task_dir, id_hash = _webdav_id_to_path(task.id, filename)
            if len(task.uploads) == 0:
                # an exists() call may be expensive, only use it once if
                # possible
                if not dav.exists(id_hash):
                    dav.mkdir(id_hash)
                    dav.mkdir(task_dir)
                elif not dav.exists(task_dir):
                    dav.mkdir(task_dir)

            dav.upload(f, path)
            task.uploads[filename] = dav._get_url(path)
        except EnvironmentError:
            task.put_attachment(filename, f.read(), mimetype)


def download_attachment(task, directory, filename, task_db=None):
    """ Uploads an attachment from the configured file storage layer. """
    if filename in task.uploads:
        dav = get_webdav()
        path = _webdav_url_to_path(task.uploads[filename], dav)
        dav.download(path, os.path.join(directory, filename))
    else:
        if task_db is None:
            task_db = get_task_database()
        attach = task.get_attachment(filename, retrieve_from_database=task_db)
        with open(os.path.join(directory, filename), 'wb') as f:
            f.write(attach['data'])


def delete_attachment(task, filename):
    """ Deletes an attachment from the configured file storage layer. """
    if filename in task.uploads:
        dav = get_webdav()
        path = _webdav_url_to_path(task.uploads[filename], dav)
        dav.delete(path)
        del task.uploads[filename]
    else:
        del task['_attachments'][filename]


def _webdav_url_to_path(url, webdav=None):
    """ Extracts the path from a webdav url. """
    if webdav is None:
        webdav = get_webdav()

    # Check that the same webdav system was used
    if not url.startswith(webdav.baseurl):
        raise EnvironmentError(
            'webdav for {0} not configured'.format(url))
    # Remove the webdav base url from the URL to get the relative path
    path = url[len(webdav.baseurl):]
    # Use absolute path
    if not path.startswith('/'):
        path = '/' + path
    return path


def _webdav_id_to_path(task_id, filename=''):
    """ Deduces the path from a task ID. """
    task_hash = '/' + task_id[5:7]
    task_dir = task_hash + '/' + task_id
    return (task_dir + '/' + filename, task_dir, task_hash)
