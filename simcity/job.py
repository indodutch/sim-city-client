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

from .management import get_current_job_id, get_job_database
from couchdb.http import ResourceConflict
from picas import Job
from picas.util import seconds
import time


def get_job(job_id=None, database=None):
    """
    Get a job from the job database.

    If no job_id is given, the job ID of the current process is used, if
    specified. If no database is provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    if job_id is None:
        job_id = get_current_job_id()
    if job_id is None:
        raise EnvironmentError("Job ID cannot be determined "
                               "(from $SIMCITY_JOBID or command-line)")
    return Job(database.get(job_id))


def queue_job(job, method, host=None, database=None):
    """
    Mark a job from the job database as being queued.

    The job is a Job object, the method a string with the type of queue being
    used, the host is the hostname that it was queued on. If no database is
    provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    try:
        job = database.save(job.queue(method, host))
    except ResourceConflict:
        job = get_job(job_id=job.id, database=database)
        return queue_job(job, method, host=host, database=database)
    else:
        if job['done'] > 0:
            return archive_job(job, database)
        else:
            return job


def start_job(database=None):
    """
    Mark a job from the job database as being started.

    The job ID of the current process is used. If no database is
    provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    try:  # EnvironmentError if job_id cannot be determined falls through
        job = get_job()
    except ValueError:  # job ID was not yet added to database
        job = Job({'_id': get_current_job_id()})

    try:
        return database.save(job.start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start_job(database)


def finish_job(job, database=None):
    """
    Mark a job from the job database as being finished.

    The job is a Job object. If no database is
    provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    try:
        job = database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        job = get_job(job_id=job.id, database=database)
        return finish_job(job, database=database)
    else:
        if job['queue'] > 0:
            return archive_job(job, database=database)
        else:
            return job


def cancel_endless_job(job, database=None):
    """
    Mark a job from the job database for cancellation.

    The job is a Job object. If no database is
    provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    try:
        job['cancel'] = seconds()
        return database.save(job)
    except ResourceConflict:
        job = get_job(job_id=job.id, database=database)
        return cancel_endless_job(job, database=database)


def archive_job(job, database=None):
    """
    Archive a job in the job database.

    The job is a Job object. If no database is
    provided, the standard job database is used.
    """
    if database is None:
        database = get_job_database()

    try:
        database.delete(job)
    except ResourceConflict:
        job = get_job(job_id=job.id, database=database)
        return archive_job(job, database=database)
    else:
        return database.save(job.archive())


def scrub_jobs(view, age=24*60*60, database=None):
    """
    Intends to update job metadata of jobs that are defunct.

    The jobs in given view will be converted to archived_jobs if their starting
    time is before given age.

    Parameters
    ----------
    view : one of (pending_jobs, active_jobs, finished_jobs)
        View to scrub jobs from
    age : int
        select jobs started at least this number of seconds ago. Set to at most
        0 to select all jobs.
    database : couchdb database, optional
        database to update the job from. Defaults to simcity.get_job_database()

    Returns
    -------
    A tuple with (the number of documents updated,
                  total number of documents in given view)
    """
    views = ['pending_jobs', 'active_jobs', 'finished_jobs']
    if view not in views:
        raise ValueError('View "%s" not one of "%s"' % (view, str(views)))

    if database is None:
        database = get_job_database()

    min_t = int(time.time()) - age
    total = 0
    updates = []
    for row in database.view(view):
        total += 1
        if age <= 0 or row.value['start'] < min_t:
            job = get_job(row.id, database=database)
            database.delete(job)
            updates.append(job.archive())

    if len(updates) > 0:
        database.save_documents(updates)

    return (len(updates), total)
