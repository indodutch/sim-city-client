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

import simcity
from couchdb.http import ResourceConflict
from picas.documents import Job


def get_job(job_id=None):
    simcity.check_init()
    if job_id is None:
        job_id = simcity.running_job_id
    if job_id is None:
        raise EnvironmentError("Job ID cannot be determined "
                               "(from $SIMCITY_JOBID or command-line)")
    return Job(simcity.job_database.get(job_id))


def start_job():
    simcity.check_init()
    try:  # EnvironmentError if job_id cannot be determined falls through
        job = get_job()
    except ValueError:  # job ID was not yet added to database
        job = Job({'_id': simcity.running_job_id})

    try:
        return simcity.job_database.save(job.start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start_job()


def finish_job(job):
    simcity.check_init()
    try:
        job = simcity.job_database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish_job(get_job())
    else:
        if job['queue'] > 0:
            return archive_job(job)
        else:
            return job


def queue_job(job, method, host=None):
    simcity.check_init()
    try:
        job = simcity.job_database.save(job.queue(method, host))
    except ResourceConflict:
        return queue_job(get_job(job.id), method)
    else:
        if job['done'] > 0:
            return archive_job(job)
        else:
            return job


def archive_job(job):
    simcity.check_init()
    try:
        simcity.job_database.delete(job)
    except ResourceConflict:
        return archive_job(get_job(job.id))
    else:
        return simcity.job_database.save(job.archive())
