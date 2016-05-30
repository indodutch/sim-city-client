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
import webdav


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
        dav.clean(task_dir)


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

    return len(updates), total


def upload_attachment(task, directory, filename, mimetype=None):
    """ Uploads an attachment using the configured file storage layer. """
    try:
        dav = get_webdav()
    except EnvironmentError:
        with open(os.path.join(directory, filename), 'rb') as f:
            task.put_attachment(filename, f.read(), mimetype)
    else:
        path, task_dir, id_hash = _webdav_id_to_path(task.id, filename)
        if len(task.uploads) == 0:
            # an exists() call may be expensive, only use it once if
            # possible
            dav.mkdir(id_hash)
            dav.mkdir(task_dir)

        try:
            dav.upload_sync(remote_path=path,
                            local_path=os.path.abspath(
                                os.path.join(directory, filename)))

            task.uploads[filename] = _webdav_path_to_url(dav, path)
        except IOError as ex:
            print(
                'WARNING: attachment {0} could not be uploaded to webdav: {1}'
                .format(filename, ex))
            with open(os.path.join(directory, filename), 'rb') as f:
                task.put_attachment(filename, f.read(), mimetype)


def download_attachment(task, directory, filename, task_db=None):
    """ Uploads an attachment from the configured file storage layer. """
    if filename in task.uploads:
        dav = get_webdav()
        path = _webdav_url_to_path(task.uploads[filename], dav)
        dav.download_sync(remote_path=path,
                          local_path=os.path.join(directory, filename))
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
        dav.clean(path)
        del task.uploads[filename]
    else:
        del task['_attachments'][filename]


def _webdav_url_to_path(url, dav=None):
    """ Extracts the path from a webdav url. """
    if dav is None:
        dav = get_webdav()

    # Check that the same webdav system was used
    if not url.startswith(dav.webdav.hostname):
        raise EnvironmentError(
            'webdav for {0} not configured'.format(url))
    # Remove the webdav base url from the URL to get the relative path
    path = url[len(dav.webdav.hostname) + len(dav.webdav.root):]
    # Use absolute path
    if not path.startswith('/'):
        path = '/' + path
    return path


def _webdav_id_to_path(task_id, filename=''):
    """ Deduces the path from a task ID. """
    if len(task_id) >= 7:
        # use hash (hopefully) by default
        task_hash = '/' + task_id[5:7]
    else:
        # use the start of the string, less effective, but hey.
        task_hash = '/' + task_id[:2]
    task_dir = task_hash + '/' + task_id
    return task_dir + '/' + filename, task_dir, task_hash


def _webdav_path_to_url(dav, path):
    """ Convert a path on a webdav to a full URL. """
    urn = webdav.urn.Urn(path)
    return '{0}{1}{2}'.format(dav.webdav.hostname, dav.webdav.root,
                              urn.quote())
