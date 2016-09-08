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

""" Manage job metadata. """

from couchdb.http import ResourceConflict
from .document import Job
from .util import seconds
import socket


class JobHandler(object):
    def __init__(self, database, current_job_id=None):
        self.database = database
        self.job_id = current_job_id

    def get(self, job_id):
        """ Get a job from the job database. """
        return Job(self.database.get(job_id))

    def prepare_for_queue(self, job, method, host_section, host=None):
        """
        Mark a job from the job database for queueing.

        The job is a Job object, the method a string with the type of queue
        being used, the host is the hostname that it was queued on.
        """
        self.update_latest(_prepare_for_queue, job, method, host_section, host)

    def queued(self, job, batch_id):
        """
        Mark a job from the job database in-queue

        The job is a Job object, the method a string with the type of queue
        being used, the host is the hostname that it was queued on.
        """
        job = self.update_latest(_queued, job, batch_id)
        if job['done'] > 0:
            return self.archive(job)
        else:
            return job

    def update_latest(self, f, job, *args, **kwargs):
        """ Perform given function on the latest version of a job. """
        while True:
            try:
                f(job, *args, **kwargs)
                return self.database.save(job)
            except ResourceConflict:
                job = self.get(job.id)

    def start(self, properties=None):
        """
        Mark a job from the job database as being started.

        The job ID of the current process is used.
        """
        if self.job_id is None:
            raise EnvironmentError("Job ID cannot be determined "
                                   "(from $SIMCITY_JOBID or command-line)")

        try:
            job = self.get(self.job_id)
        except ValueError:  # job ID was not yet added to database
            job = Job({'_id': self.job_id})

        self.update_latest(_update_job, job, properties)

    def finish(self, job, tasks_processed=None):
        """
        Mark a job from the job database as being finished.

        The job is a Job object.
        """
        job = self.update_latest(_finish, job, tasks_processed)
        if job['queue'] > 0:
            return self.archive(job)
        else:
            return job

    def cancel_endless(self, job):
        """
        Mark a job from the job database for cancellation.

        The job is a Job object.
        """
        self.update_latest(_cancel_job, job)

    def archive(self, job):
        """
        Archive a job in the job database.

        The job is a Job object.
        """
        self.update_latest(_archive, job)


def _prepare_for_queue(job, method, host_section, host=None):
    """ Save that the job was queued. """
    job['method'] = method
    job['host_section'] = host_section
    if host is not None:
        job['hostname'] = host
    job['queue'] = seconds()


def _queued(job, batch_id):
    job['batch_id'] = batch_id


def _start(job):
    """ Save that the job has started. """
    job['start'] = seconds()
    job['done'] = 0
    job['archive'] = 0
    job['hostname'] = socket.gethostname()


def _finish(job, tasks_processed):
    """ Save that the job is done. """
    if tasks_processed is not None:
        job['tasks_processed'] = tasks_processed
    job['done'] = seconds()


def _archive(job):
    """ Move the job to an archived state. """
    if job['done'] <= 0:
        job['done'] = seconds()
    job['archive'] = seconds()


def _update_job(job, properties):
    if properties is not None:
        job.update(properties)


def _cancel_job(job):
    job['cancel'] = seconds()
