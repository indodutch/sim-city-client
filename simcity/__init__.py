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
SIM-CITY client helps running tasks on any infrastructure.

SIM-CITY client has a CouchDB backend to store task input and output and
job metadata. A task runner can be started manually on any computer with
internet connection and with the desired executable installed in the right
path. Otherwise SIM-CITY client will submits jobs to remote infrastructure
using SSH and it will starts a qsub job there, or it uses Osmium to submit
the job. Output files can be stored in the CouchDB database or on a WebDAV
server.
"""

from .actors import JobActor
from .dav import RestRequests
from .ensemble import ensemble_view
from .worker import ExecuteWorker
from .job import (get_job, start_job, queue_job, finish_job, archive_job,
                  cancel_endless_job, scrub_jobs)
from .integration import (overview_total, run_task, submit_if_needed,
                          submit_while_needed, check_job_status,
                          check_task_status)
from .management import (get_config, init, get_task_database, get_job_database,
                         get_current_job_id, set_current_job_id,
                         create, create_views, uses_webdav, get_webdav,
                         load_config_database)
from .submit import (submit, Adaptor, OsmiumAdaptor, xenon_support,
                     SSHAdaptor, kill, status)
from .task import (add_task, get_task, delete_task, scrub_tasks,
                   upload_attachment, download_attachment, delete_attachment)
from .config import Config, CouchDBConfig, FileConfig
from .document import Task, Job, Document, User
from .database import CouchDB
from .iterator import (ViewIterator, TaskViewIterator, EndlessViewIterator,
                       PrioritizedViewIterator)
from .util import parse_parameters
from .version import __version__, __version_info__

__all__ = [
    'Adaptor',
    'add_task',
    'archive_job',
    'cancel_endless_job',
    'check_job_status',
    'check_task_status',
    'Config',
    'CouchDB',
    'CouchDBConfig',
    'create',
    'create_views',
    'delete_attachment',
    'delete_task',
    'Document',
    'download_attachment',
    'EndlessViewIterator',
    'ensemble_view',
    'ExecuteWorker',
    'FileConfig',
    'finish_job',
    'get_config',
    'get_current_job_id',
    'get_job',
    'get_job_database',
    'get_task',
    'get_task_database',
    'get_webdav',
    'init',
    'Job',
    'JobActor',
    'kill',
    'load_config_database',
    'OsmiumAdaptor',
    'overview_total',
    'parse_parameters',
    'PrioritizedViewIterator',
    'queue_job',
    'RestRequests',
    'run_task',
    'scrub_jobs',
    'scrub_tasks',
    'set_current_job_id',
    'SSHAdaptor',
    'start_job',
    'status',
    'submit',
    'submit_if_needed',
    'submit_while_needed',
    'Task',
    'TaskViewIterator',
    'upload_attachment',
    'User',
    'uses_webdav',
    'ViewIterator',
    'xenon_support',
    '__version__',
    '__version_info__',
]

if xenon_support:
    from .submit import XenonAdaptor  # noqa: F401
    __all__.append('XenonAdaptor')
