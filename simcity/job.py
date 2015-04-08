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

from .management import get_current_job_id, get_job_database
from couchdb.http import ResourceConflict
from picas.documents import Job


def get_job(job_id=None, database=None):
    if database is None:
        database = get_job_database()

    if job_id is None:
        job_id = get_current_job_id()
    if job_id is None:
        raise EnvironmentError("Job ID cannot be determined "
                               "(from $SIMCITY_JOBID or command-line)")
    return Job(database.get(job_id))


def queue_job(job, method, host=None, database=None):
    if database is None:
        database = get_job_database()

    try:
        job = database.save(job.queue(method, host))
    except ResourceConflict:
        return queue_job(get_job(job.id), method, host=host,
                         database=database)
    else:
        if job['done'] > 0:
            return archive_job(job, database)
        else:
            return job


def start_job(database=None):
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
    if database is None:
        database = get_job_database()

    try:
        job = database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish_job(get_job(), database=database)
    else:
        if job['queue'] > 0:
            return archive_job(job, database=database)
        else:
            return job


def archive_job(job, database=None):
    if database is None:
        database = get_job_database()

    try:
        database.delete(job)
    except ResourceConflict:
        return archive_job(get_job(job.id), database=database)
    else:
        return database.save(job.archive())
