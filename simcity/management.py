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
    _current_job_id = os.environ['SIMCITY_JOBID']
except:
    _current_job_id = None

_config = None
is_initialized = False
_is_initializing = True
_task_db = None
_job_db = None


def get_config():
    _check_init()
    return _config


def get_task_database():
    _check_init()
    return _task_db


def get_job_database():
    _check_init()
    return _job_db


def get_current_job_id():
    return _current_job_id


def set_current_job_id(job_id):
    global _current_job_id
    _current_job_id = job_id


def _check_init():
    if not is_initialized:
        raise EnvironmentError(
            "Databases are not initialized yet, please provide a valid "
            "configuration file to simcity.init()")


def init(configfile, job_id=None):
    global is_initialized, _is_initializing, _config, _task_db
    global _job_db, _current_job_id

    if job_id is not None:
        _current_job_id = job_id

    try:
        _config = Config(configfile)
    except:
        # default initialization may fail
        if not _is_initializing:
            raise
    else:
        try:
            _task_db = _load_database('task-db')
        except:
            if not _is_initializing:
                raise

        try:
            _job_db = _load_database('job-db')
        except EnvironmentError:
            # job database not explicitly configured
            _job_db = _task_db
        except:
            if not _is_initializing:
                raise

        is_initialized = True

    if _is_initializing:
        _is_initializing = False


def _load_database(name):
    try:
        cfg = get_config().section(name)
    except NoSectionError:
        raise EnvironmentError(
            "Configuration file %s does not contain '%s' section" %
            (get_config().filename, name))

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
    views = ['todo', 'locked', 'error', 'done',
             'finished_jobs', 'active_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in get_task_database().view('overview_total', group=True):
        num[view.key] = view.value

    if get_job_database() is not get_task_database():
        for view in get_job_database().view('overview_total', group=True):
            num[view.key] = view.value

    return num
