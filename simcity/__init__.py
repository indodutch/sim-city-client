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

from .actors import ExecuteActor
from .job import (get_job, start_job, queue_job, finish_job, archive_job,
                  cancel_endless_job, scrub_jobs)
from .integration import overview_total, run_task
from .management import (get_config, init, is_initialized,
                         get_task_database, get_job_database,
                         get_current_job_id, set_current_job_id,
                         create, create_views)
from .submit import (submit, submit_if_needed, Submitter, OsmiumSubmitter,
                     SSHSubmitter)
from .task import (add_task, get_task, delete_task, scrub_tasks,
                   upload_attachment, download_attachment, delete_attachment)
from .util import Config

__all__ = [
    'ExecuteActor',
    'get_job', 'start_job', 'queue_job', 'finish_job', 'archive_job',
    'cancel_endless_job', 'scrub_jobs',
    'overview_total', 'run_task',
    'get_config', 'init', 'is_initialized',
    'get_task_database', 'get_job_database',
    'create', 'create_views',
    'get_current_job_id', 'set_current_job_id',
    'submit', 'submit_if_needed', 'Submitter', 'OsmiumSubmitter',
    'SSHSubmitter',
    'add_task', 'get_task', 'delete_task', 'scrub_tasks',
    'upload_attachment', 'download_attachment', 'delete_attachment',
    'Config',
]

init(None)
