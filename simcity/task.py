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
from .util import data_content_type, file_content_type
import time
import os
import io


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

    if len(task.files) > 0:
        # Not a for loop, since task.files is modified in delete_attachment.
        while len(task.files) > 0:
            filename = next(iter(task.files.keys()))
            delete_attachment(task, filename)

        dav = get_webdav()
        task_dir = _webdav_id_to_path(task.id)[1]
        dav.delete(task_dir, ignore_not_existing=True)


def scrub_tasks(view, age=24 * 60 * 60, database=None):
    """
    Intends to update task metadata of defunct tasks.

    The tasks in given view will returned to jobs to be processed
    if their starting time is before given age.

    Parameters
    ----------
    view : one of (in_progress, error)
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
    views = ['in_progress', 'error']
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


def upload_attachment(task, directory, filename, content_type=None):
    """ Uploads an attachment using the configured file storage layer. """
    file_path = os.path.abspath(os.path.join(directory, filename))

    # determine whether a json type is geojson
    if content_type is None:
        content_type = file_content_type(filename, file_path)

    with open(file_path, 'rb') as f:
        _put_attachment(task, filename, f, os.stat(file_path).st_size,
                        content_type)


def _put_attachment(task, filename, f, length, content_type=None):
    try:
        dav = get_webdav()
    except EnvironmentError:
        task.put_attachment(filename, f.read(), content_type)
    else:
        path, task_dir, id_hash = _webdav_id_to_path(task.id, filename)
        try:
            if len(task.files) == 0:
                dav.mkdir(id_hash, ignore_existing=True)
                dav.mkdir(task_dir, ignore_existing=True)

            dav.put(path, f, content_type=content_type)

            task.files[filename] = {
                'url': dav.path_to_url(path),
                'length': length,
            }
            if content_type is not None:
                task.files[filename]['content_type'] = content_type

        except IOError as ex:
            print(
                'WARNING: attachment {0} could not be uploaded to webdav: {1}'
                .format(filename, ex))
            task.put_attachment(filename, f.read(), content_type)


def write_attachment(task, filename, data, content_type=None):
    """
    Writes data as an attachment using the configured file storage layer.
    @param data: bytes data
    @param task: task that the data belongs to
    @param filename: filename that the data should take
    @param content_type: content type of the data. If None, it is guessed from
        the file name.
    """
    # determine whether a json type is geojson
    if content_type is None:
        content_type = data_content_type(filename, data)

    _put_attachment(task, filename, io.BytesIO(data), len(data),
                    content_type)


def read_attachment(task, filename, task_db=None):
    """
    Reads an attachment from the configured file storage layer as bytes data.
    """
    if filename in task.files:
        url = task.files[filename]['url']
        dav = get_webdav()
        return dav.get(dav.url_to_path(url))
    else:
        if task_db is None:
            task_db = get_task_database()
        attach = task.get_attachment(filename, retrieve_from_database=task_db)
        return attach['data']


def download_attachment(task, directory, filename, task_db=None):
    """ Uploads an attachment from the configured file storage layer. """
    if filename in task.files:
        dav = get_webdav()
        url = task.files[filename]['url']
        file_path = os.path.join(directory, filename)
        dav.download(dav.url_to_path(url), file_path)
    else:
        if task_db is None:
            task_db = get_task_database()
        attach = task.get_attachment(filename, retrieve_from_database=task_db)
        with open(os.path.join(directory, filename), 'wb') as f:
            f.write(attach['data'])


def delete_attachment(task, filename):
    """ Deletes an attachment from the configured file storage layer. """
    if filename in task.files:
        dav = get_webdav()
        dav.delete(dav.url_to_path(task.files[filename]['url']),
                   ignore_not_existing=True)
        del task.files[filename]
    else:
        task.delete_attachment(filename)


def _webdav_id_to_path(task_id, filename=''):
    """ Deduces the path from a task ID. """
    if len(task_id) >= 7:
        # use hash (hopefully) by default
        task_hash = task_id[5:7]
    else:
        # use the start of the string, less effective, but hey.
        task_hash = task_id[:2]
    task_dir = task_hash + '/' + task_id
    return task_dir + '/' + filename, task_dir, task_hash
