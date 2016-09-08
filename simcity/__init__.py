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
from .dav import (RestRequests, Attachments)
from .ensemble import ensemble_view
from .worker import ExecuteWorker
from .job import JobHandler
from .management import Barbecue, load_config, load_database, load_webdav
from .submit import (SubmitHandler, SubmitAdaptor, xenon_support)
from .task import TaskHandler
from .config import Config, CouchDBConfig, FileConfig
from .document import Task, Job, Document, User
from .database import CouchDB
from .iterator import (ViewIterator, TaskViewIterator, EndlessViewIterator,
                       PrioritizedViewIterator)
from .util import parse_parameters
from .version import __version__, __version_info__

__all__ = [
    'Attachments',
    'Barbecue',
    'Config',
    'CouchDB',
    'CouchDBConfig',
    'Document',
    'EndlessViewIterator',
    'ensemble_view',
    'ExecuteWorker',
    'FileConfig',
    'Job',
    'JobActor',
    'load_config',
    'load_database',
    'load_webdav',
    'parse_parameters',
    'PrioritizedViewIterator',
    'RestRequests',
    'SubmitAdaptor',
    'SubmitHandler',
    'Task',
    'TaskViewIterator',
    'User',
    'ViewIterator',
    'xenon_support',
    '__version__',
    '__version_info__',
]

if xenon_support:
    from .submit import XenonAdaptor  # noqa: F401
    __all__.append('XenonAdaptor')
