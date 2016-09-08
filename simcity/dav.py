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
"""
Attachment handler with basic WebDAV implementation

Reason this exists: easywebdav is not Python 3 compatible, webdavclient is
flaky and not compatible with (at least) Beehub.
"""
import requests
import os
import io
from .util import file_content_type, data_content_type


def verify(acceptable_statuses, response, message):
    """
    Verify that the request gave an acceptable status_code and raises an
    IOError otherwise.
    """
    if response.status_code not in acceptable_statuses:
        header_str = '\n'.join(['{0}: {1}'.format(k, v)
                                for k, v in response.headers.lower_items()])
        raise IOError('{0}: HTTP status code {1}\nHeaders:\n{2}'
                      .format(message, response.status_code, header_str))


class RestRequests(object):
    """
    Rest requests with a base URL.

    Each method takes **kwargs as an argument, which will be passed to the
    requests.request() function.
    """
    def __init__(self, base_url, auth=None, **kwargs):
        """
        @param base_url: base url of the service
        @param auth: requests authentication tuple
        @param kwargs: kwargs to pass on to the requests.request() function.
            These can be updated in each subsequent function.
        """
        self.base_url = base_url.rstrip('/')
        self.auth = auth
        self.kwargs = kwargs

    def exists(self, path, **kwargs):
        """ Whether the path exists. """
        kwargs.update(self.kwargs)
        response = requests.head(self.path_to_url(path), auth=self.auth,
                                 allow_redirects=True, **kwargs)
        return response.status_code == 200

    def put(self, path, data, content_type=None, content_length=None,
            **kwargs):
        """
        Put bytes, dict, or a file pointer.
        If a file pointer is given, the content_length needs to be specified,
        in bytes.
        """
        kwargs.update(self.kwargs)
        if content_type is None:
            headers = None
        else:
            headers = {'content-type': content_type}

        if content_length is None and hasattr(data, 'read'):
            raise ValueError('Given file data needs specified content_length.')

        # requests will make a multipart request for 0-length files, where both
        # parts have both with 0 data. This may fail on some WebDAV servers
        # (e.g. Apache httpd). Using an empty string resolves this.
        if content_length == 0 and hasattr(data, 'read'):
            response = requests.put(self.path_to_url(path), data=b'',
                                    auth=self.auth, headers=headers, **kwargs)
        else:
            response = requests.put(self.path_to_url(path), data=data,
                                    auth=self.auth, headers=headers, **kwargs)

        verify((201, 204), response, 'Failed to upload file {0}'.format(path))

    def mkdir(self, path, ignore_existing=False, **kwargs):
        """
        Make a new directory. Set ignore_existing to not throw an error if
        the directory already exists.
        """
        kwargs.update(self.kwargs)
        response = requests.request('MKCOL', self.path_to_url(path),
                                    auth=self.auth, **kwargs)
        if ignore_existing:
            acceptable_status = 201, 405
        else:
            acceptable_status = 201,
        verify(acceptable_status, response,
               'Failed to create directory {0}'.format(path))

    def delete(self, path, ignore_not_existing=False, **kwargs):
        """
        Recursively delete a path. Set ignore_not_existing to not throw an
        error if given path does not exist.
        """
        kwargs.update(self.kwargs)
        response = requests.delete(self.path_to_url(path), auth=self.auth,
                                   **kwargs)
        if ignore_not_existing:
            acceptable_status = 204, 404
        else:
            acceptable_status = 204,
        verify(acceptable_status, response,
               'Failed to delete {0}'.format(path))

    def get(self, path, **kwargs):
        """ Get path contents as bytes. """
        kwargs.update(self.kwargs)
        response = requests.get(self.path_to_url(path), auth=self.auth,
                                **kwargs)
        verify((200,), response, 'Failed to get file {0}'.format(path))
        return response.content

    def download(self, path, file_path, chunk_size=1024 * 1024, **kwargs):
        """ Download path to file_path. """
        kwargs.update(self.kwargs)
        response = requests.get(self.path_to_url(path), stream=True,
                                auth=self.auth, **kwargs)

        verify((200,), response, 'Failed to get file {0}'.format(path))
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    def path_to_url(self, path):
        """ Given a path relative to the base_url, returns the full URL."""
        return '{0}/{1}'.format(self.base_url, path.lstrip('/'))

    def url_to_path(self, url):
        """
        Given an URL prefixed with the base_url, return the relative path.
        """
        if not url.startswith(self.base_url):
            raise ValueError('URL {0} cannot be translated to webdav at {1}'
                             .format(url, self.base_url))
        return url[len(self.base_url):].lstrip('/')


class CouchAttachmentHandler(object):
    def __init__(self, db):
        self.db = db

    def put(self, task, filename, f, length, content_type=None):
        task.put_attachment(filename, f.read(), content_type)

    def contains(self, task, name):
        return name in task.attachments

    def delete(self, task, name):
        task.delete_attachment(name)

    def get(self, task, name):
        return task.get_attachment(task, name, retrieve_from_database=self.db)

    def download(self, task, name, directory):
        attach = self.get(task, name)
        with open(os.path.join(directory, name), 'wb') as f:
            f.write(attach['data'])


class RestAttachmentHandler(object):
    def __init__(self, dav):
        self.dav = dav

    def put(self, task, filename, f, length, content_type=None):
        path, task_dir, id_hash = _webdav_id_to_path(task.id, filename)

        if len(task.files) == 0:
            self.dav.mkdir(id_hash, ignore_existing=True)
            self.dav.mkdir(task_dir, ignore_existing=True)

        self.dav.put(path, f, content_type=content_type,
                     content_length=length)

        task.files[filename] = {
            'url': self.dav.path_to_url(path),
            'length': length,
        }
        if content_type is not None:
            task.files[filename]['content_type'] = content_type

    def contains(self, task, name):
        return (name in task.files and
                task.files[name]['url'].startswith(self.dav.base_url))

    def delete(self, task, name):
        self.dav.delete(self.dav.url_to_path(task.files[name]['url']),
                        ignore_not_existing=True)
        del task.files[name]

    def get(self, task, name):
        url = task.files[name]['url']
        return self.dav.get(self.dav.url_to_path(url))

    def download(self, task, directory, name):
        url = task.files[name]['url']
        file_path = os.path.join(directory, name)
        self.dav.download(self.dav.url_to_path(url), file_path)


class Attachments(object):
    def __init__(self):
        self.handlers = []

    def containing_handler(self, task, name):
        for handler in self.handlers:
            if handler.contains(task, name):
                return handler
        raise IOError("attachment {0} of task {1} does not have a suitable "
                      "file handler configured.".format(name, task.id))

    def upload(self, task, filename, directory, content_type=None):
        """ Uploads an attachment using the configured file storage layer. """
        file_path = os.path.abspath(os.path.join(directory, filename))

        # determine whether a json type is geojson
        if content_type is None:
            content_type = file_content_type(filename, file_path)

        with open(file_path, 'rb') as f:
            for handler in self.handlers:
                try:
                    handler.put(task, filename, f, os.stat(file_path).st_size,
                                content_type)
                    return
                except IOError:
                    pass
            else:
                raise IOError("Cannot upload attachment {0} of task {1} with"
                              "any configured file handler.".format(filename,
                                                                    task.id))

    def put(self, task, filename, data, content_type=None):
        """
        Writes data as an attachment using the configured file storage layer.
        @param data: bytes data
        @param task: task that the data belongs to
        @param filename: filename that the data should take
        @param content_type: content type of the data. If None, it is guessed from
            the file name.
        """
        if content_type is None:
            content_type = data_content_type(filename, data)

        for handler in self.handlers:
            try:
                handler.put(task, filename, io.BytesIO(data), len(data),
                            content_type)
                return
            except IOError:
                pass
        else:
            raise IOError("Cannot upload attachment {0} of task {1} with"
                          "any configured file handler.".format(name,
                                                                task.id))

    def get(self, task, name):
        """
        Reads an attachment from the configured file storage layer as bytes data.
        """
        return self.containing_handler(task, name).get(task, name)

    def download(self, task, name, directory):
        """ Uploads an attachment from the configured file storage layer. """
        self.containing_handler(task, name).download(task, name, directory)

    def delete(self, task, name):
        """ Deletes an attachment from the configured file storage layer. """
        self.containing_handler(task, name).delete(task, name)


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

