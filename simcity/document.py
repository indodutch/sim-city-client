# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center, Jan Bot
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
# limitations under the License."""

"""
CouchDB document types.
"""
import socket
from .util import seconds
import mimetypes
import base64
import traceback
from uuid import uuid4
import couchdb


class Document(couchdb.Document):
    """ A CouchDB document """

    def __init__(self, data=None, base=None):
        super(Document, self).__init__()

        if data is None:
            data = {}

        # Data is not from the database
        if '_rev' not in data and base is not None:
            self.update(base)

        self.update(data)

    @property
    def attachments(self):
        """ Raw CouchDB attachments. """
        return self.setdefault('_attachments', {})

    @property
    def value(self):
        """ DEPRECATED. Returns itself."""
        return self

    def put_attachment(self, name, data, mimetype=None):
        """
        Put an attachment in the document.

        The attachment data must be provided as str in Python 2 and bytes in
        Python 3.

        The mimetype, if not provided, is guessed from the filename and
        defaults to text/plain.
        """
        if mimetype is None:
            mimetype, encoding = mimetypes.guess_type(name)
            if mimetype is None:
                mimetype = 'text/plain'

        b64data = base64.b64encode(data)
        self.attachments[name] = {
            'content_type': mimetype, 'data': b64data}

    def get_attachment(self, name, retrieve_from_database=None):
        """ Gets an attachment dict from the document.
        Attachment data may not have been copied over from the
        database, in that case it will have an md5 checksum.
        A CouchDB database may be set in retrieve_from_database to retrieve
        the data if it is not present.

        The attachment data will be returned as str in Python 2 and bytes in
        Python 3.

        Raises KeyError if attachment does not exist.
        """
        # Copy all attributes except data, it may be very large
        attachment = {}
        for key in self.attachments[name]:
            if key != 'data':
                attachment[key] = self.attachments[name][key]

        if 'data' in self.attachments[name]:
            attachment['data'] = base64.b64decode(
                self.attachments[name]['data'])
        elif retrieve_from_database is not None:
            db = retrieve_from_database.db
            f_attach = db.get_attachment(self.id, name)
            try:
                attachment['data'] = f_attach.read()
            finally:
                f_attach.close()

            b64data = base64.b64encode(attachment['data'])
            self.attachments[name]['data'] = b64data

        return attachment

    def delete_attachment(self, name):
        """ Deletes attachment from document
        @param name: document name
        @raise KeyError: if document name does not exist
        """
        del self.attachments[name]
        return self

    def _update_hostname(self):
        """ Update the hostname value to the current host. """
        self['hostname'] = socket.gethostname()
        return self

    def list_files(self):
        """ All attachment names associated to a task. """
        return list(self.attachments.keys())


class User(Document):
    """ CouchDB user """
    def __init__(self, username, password, roles=None, data=None):
        super(User, self).__init__(
            data=data if data is not None else {},
            base={
                '_id': 'org.couchdb.user:{0}'.format(username),
                'name': username,
                'type': 'user',
                'password': password,
                'roles': roles if roles is not None else [],
            })


class Task(Document):
    """ Class to manage task modifications with. """

    def __init__(self, task=None):
        super(Task, self).__init__(
            data=task,
            base={
                'type': 'task',
                'lock': 0,
                'done': 0,
                'hostname': '',
                'scrub_count': 0,
                'input': {},
                'output': {},
                'files': {},
                'error': [],
            })
        if self.id is None:
            self['_id'] = 'task_' + uuid4().hex

    def lock(self, job_id):
        """Function which modifies the task such that it is in progress.
        :@param job_id: job id that is processing the task
        """
        self['lock'] = seconds()
        self['job'] = job_id
        return self._update_hostname()

    def done(self):
        """Function which modifies the task such that it is closed for ever
        to the view that has supplied it.
        """
        self['done'] = seconds()
        return self

    @property
    def input(self):
        """ Get input """
        return self['input']

    @input.setter
    def input(self, value):
        """ Set input """
        self['input'] = value

    @property
    def output(self):
        """Get the output from the RunActor."""
        return self['output']

    @output.setter
    def output(self, output):
        """Add the output of the RunActor to the task.
        """
        self['output'] = output

    @property
    def files(self):
        """ Associated files that were uploaded. """
        return self.setdefault('files', {})

    @files.setter
    def files(self, files):
        self['files'] = files

    def list_files(self):
        """ All attachment names associated to a task. """
        return list(self.files.keys()) + list(self.attachments.keys())

    def scrub(self):
        """
        Task scrubber: makes sure a task can be handed out again if it was
        in progress more than t seconds ago.
        """
        self['scrub_count'] = 1 + self.setdefault('scrub_count', 0)
        self['done'] = 0
        self['lock'] = 0
        return self._update_hostname()

    def error(self, msg=None, exception=None):
        """ Set an error for the task. """
        error = {'time': seconds()}
        if msg is not None:
            error['message'] = str(msg)

        if exception is not None:
            error['exception'] = traceback.format_exc()

        self['lock'] = -1
        self['done'] = -1
        self.setdefault('error', []).append(error)
        return self

    def has_error(self):
        """ Whether the task had an error. """
        return self['lock'] == -1

    def get_errors(self):
        """ Get the list of errors. """
        return self.setdefault('error', [])

    def is_done(self):
        """ Whether the task successfully finished. """
        return self['done'] != 0


class Job(Document):
    """ Class to manage a CouchDB job entry. """
    def __init__(self, job):
        super(Job, self).__init__(
            data=job,
            base={
                'type': 'job',
                'hostname': '',
                'start': 0,
                'done': 0,
                'queue': 0,
                'method': '',
                'archive': 0,
            })
        if self.id is None:
            raise ValueError('Job ID must be set')

    def queue(self, method, host=None):
        """ Save that the job was queued. """
        self['method'] = method
        if host is not None:
            self['hostname'] = host
        self['queue'] = seconds()
        return self

    def start(self):
        """ Save that the job has started. """
        self['start'] = seconds()
        self['done'] = 0
        self['archive'] = 0
        return self._update_hostname()

    def finish(self):
        """ Save that the job is done. """
        self['done'] = seconds()
        return self

    def archive(self):
        """ Move the job to an archived state. """
        if self['done'] <= 0:
            self['done'] = seconds()
        self['archive'] = seconds()
        return self

    def is_done(self):
        """ Whether the job is done. """
        return self['done'] != 0
