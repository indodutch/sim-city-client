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

""" Integrates the task and job API. """
from .document import Job, Task
from .management import get_task_database, get_job_database
from .job import get_job, archive_job
from .task import add_task, get_task
from .submit import submit, status, Adaptor
from .util import seconds
import couchdb
import time


def run_task(task_properties, host, max_jobs, polling_time=None):
    """
    Run a single task, starting a job if necessary.
    Waits for the task to finish if polling_time is specified.

    Parameters
    ----------
    task_properties : dict
        properties that the given task will include.
    host : str
        host name to start a new job on, if not enough jobs are running
    max_jobs : int
        maximum number of jobs that may run, even with a larger number of tasks
    polling_time : int
        if not none, keep polling every polling_time seconds, until the job is
        done
    """
    task = add_task(task_properties)
    job = submit_if_needed(host, max_jobs)

    if polling_time is not None:
        while task['done'] == 0:
            time.sleep(polling_time)
            task = get_task(task.id)

    return task, job


def overview_total():
    """
    Overview of all tasks and jobs.

    Returns a dict with the numbers of each type of job and task.
    """
    views = ['pending', 'in_progress', 'error', 'done',
             'finished_jobs', 'running_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in get_task_database().view('overview_total', group=True):
        num[view.key] = view.value

    if get_job_database() is not get_task_database():
        for view in get_job_database().view('overview_total', group=True):
            num[view.key] = view.value

    return num


def submit_if_needed(host_id, max_jobs, adaptor=None):
    """
    Submit a new job if not enough jobs are already running or queued.

    Host configuration is extracted from an entry in the global config file.
    """
    num = overview_total()

    num_jobs = num['running_jobs'] + num['pending_jobs']
    num_tasks = num['pending'] + num['in_progress']
    if num_jobs < min(num_tasks, max_jobs):
        return submit(host_id, adaptor)
    else:
        return None


def submit_while_needed(host_id, max_jobs, adaptor=None, dry_run=False):
    """
    Submits new job while not enough jobs are already running or queued.

    Host configuration is extracted from an entry in the global config file.
    """
    num = overview_total()
    num_jobs = num['running_jobs'] + num['pending_jobs']
    num_tasks = num['pending'] + num['in_progress']

    new_jobs = max(0, min(num_tasks, max_jobs) - num_jobs)

    if dry_run:
        return [None] * new_jobs
    else:
        return [submit(host_id, adaptor) for _ in range(new_jobs)]


def check_job_status(dry_run=False, database=None):
    """
    Check the current job status of jobs that the database considers active.
    If dry_run is false, modify incongruent job statuses.
    @param dry_run: do not modify job
    @param database: job database
    @return: list of jobs that are archived
    """
    if database is None:
        database = get_job_database()

    jobs = [get_job(row.id) for row in database.view('active_jobs')]
    jobs = [job for job in jobs if job.type == 'job']
    job_status = status(jobs)

    new_jobs = []
    five_days = 5 * 24 * 60 * 60
    for stat, job in zip(job_status, jobs):
        if ((stat is None and seconds() - job['queue'] > five_days) or
                stat == Adaptor.DONE):
            if dry_run:
                new_jobs.append(job)
            else:
                new_jobs.append(archive_job(job))

    return new_jobs


def check_task_status(dry_run=False, database=None):
    """
    Check the current task status of in_progress tasks  against the job status
    of the job that it is supposed to be executing it.
    If dry_run is false, modify incongruent task statuses.
    @param dry_run: do not modify task
    @param database: task database
    @return: list of tasks that are marked as in error because their job is not
        running
    """
    if database is None:
        database = get_task_database()

    has_failed_saves = True
    new_tasks = []
    while has_failed_saves:
        has_failed_saves = False

        for row in database.view('in_progress'):
            task = get_task(row.id)
            job = get_job(task['job'])
            if job.is_done():
                if dry_run:
                    new_tasks.append(task)
                else:
                    try:
                        new_tasks.append(database.save(task))
                    except couchdb.http.ResourceConflict:
                        has_failed_saves = True

    return new_tasks


def scrub(view, age=24 * 60 * 60, database=None):
    """
    Intends to update job metadata of defunct jobs or tasks.

    If their starting time is before given age, Tasks that were locked will be
    unlocked and Jobs will be archived.

    Parameters
    ----------
    view : {in_progress, error, pending_jobs, running_jobs, finished_jobs}
        View to scrub jobs from
    age : int
        select jobs started at least this number of seconds ago. Set to at most
        0 to select all documents.
    database : couchdb database, optional
        database to update the documents from. Defaults to
        simcity.get_{job,task}_database()

    Returns
    -------
    A tuple with (the number of documents updated,
                  total number of documents in given view)
    """
    task_views = ['in_progress', 'error']
    job_views = ['pending_jobs', 'running_jobs', 'finished_jobs']
    if view in task_views:
        is_task = True
        age_var = 'lock'
    elif view in job_views:
        is_task = False
        age_var = 'start'
    else:
        raise ValueError('View "%s" not one of "%s"' % (view, str(task_views +
                                                                  job_views)))
    if database is None:
        database = get_task_database() if is_task else get_job_database()

    min_t = int(time.time()) - age
    total = 0
    updates = []
    for row in database.view(view):
        total += 1
        if age <= 0 or row.value[age_var] < min_t:
            doc = database.get(row.id)
            if is_task:
                doc = Task(doc).scrub()
            else:
                doc = Job(doc).archive()
            updates.append(doc)

    if len(updates) > 0:
        database.save_documents(updates)

    return len(updates), total
