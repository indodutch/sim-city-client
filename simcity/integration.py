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

from .management import get_task_database, get_job_database
from .task import add_task, get_task
from .submit import submit_if_needed
import time


def run_task(task_properties, host, max_jobs, polling_time=None):
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
