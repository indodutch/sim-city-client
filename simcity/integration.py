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

from .management import get_task_database, get_job_database
from .task import add_task, get_task
from .submit import submit_if_needed
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

    return (task, job)


def overview_total():
    """
    Overview of all tasks and jobs.

    Returns a dict with the numbers of each type of job and task.
    """
    views = ['todo', 'locked', 'error', 'done',
             'finished_jobs', 'active_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in get_task_database().view('overview_total', group=True):
        num[view.key] = view.value

    if get_job_database() is not get_task_database():
        for view in get_job_database().view('overview_total', group=True):
            num[view.key] = view.value

    return num
