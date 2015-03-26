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

from picas.clients import CouchDB
from .util import Config

import os
from ConfigParser import NoSectionError

try:
    running_job_id = os.environ['SIMCITY_JOBID']
except:
    running_job_id = None

config = None
is_initialized = False
_is_initializing = True
task_database = None
job_database = None


def check_init():
    if not is_initialized:
        raise EnvironmentError(
            "Databases are not initialized yet, please provide a valid "
            "configuration file to simcity.init()")


def init(configfile):
    global is_initialized, _is_initializing, config, task_database
    global job_database

    try:
        config = Config(configfile)
    except:
        # default initialization may fail
        if not _is_initializing:
            raise
    else:
        try:
            task_database = _load_database('task-db')
        except:
            if not _is_initializing:
                raise

        try:
            job_database = _load_database('job-db')
        except EnvironmentError:
            # job database not explicitly configured
            job_database = task_database
        except:
            if not _is_initializing:
                raise

        is_initialized = True

    if _is_initializing:
        _is_initializing = False


def _load_database(name):
    try:
        cfg = config.section(name)
    except NoSectionError:
        raise EnvironmentError(
            "Configuration file %s does not contain '%s' section" %
            (config.filename, name))

    try:
        return CouchDB(
            url=cfg['url'],
            db=cfg['database'],
            username=cfg['username'],
            password=cfg['password'])
    except IOError as ex:
        raise IOError("Cannot establish connection with %s CouchDB <%s>: %s" %
                      (name, cfg['url'], str(ex)))


def overview_total():
    check_init()

    views = ['todo', 'locked', 'error', 'done',
             'finished_jobs', 'active_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in task_database.view('overview_total', group=True):
        num[view.key] = view.value

    if job_database is not task_database:
        for view in job_database.view('overview_total', group=True):
            num[view.key] = view.value

    return num
