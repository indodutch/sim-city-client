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
from numbers import Number

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
    """ Get the global SIM-CITY configuration. """
    _check_init(_config)
    return _config


def set_config(cfg):
    """ Set the global SIM-CITY configuration. """
    global _config
    _config = cfg
    _init_databases()


def set_task_database(database):
    """ Set the global SIM-CITY task database. """
    global _task_db
    _task_db = database
    _reset_init()


def set_job_database(database):
    """ Set the global SIM-CITY job database. """
    global _job_db
    _job_db = database
    _reset_init()


def get_task_database():
    """ Get the global SIM-CITY task database. """
    _check_init(_task_db)
    return _task_db


def get_job_database():
    """ Get the global SIM-CITY job database. """
    _check_init(_job_db)
    return _job_db


def get_current_job_id():
    """ Get the global SIM-CITY job id of the currently running job. """
    return _current_job_id


def set_current_job_id(job_id):
    """ Set the global SIM-CITY job id of the currently running job. """
    global _current_job_id
    _current_job_id = job_id


def _check_init(myvalue=None):
    if myvalue is None:
        raise EnvironmentError(
            "Databases are not initialized yet, please provide a valid "
            "configuration file to simcity.init()")


def init(config, job_id=None):
    """
    Initialize the SIM-CITY infrastructure.

    The config is the INI file containing all needed global configuration or
    a simcity.Config object.
    """
    global _is_initializing, _config, _current_job_id

    if job_id is not None:
        _current_job_id = job_id

    if isinstance(config, Config):
        _config = config
        _init_databases()
    else:
        try:
            _config = Config(config)
        except ValueError:
            # default initialization may fail
            if not _is_initializing:
                raise
        else:
            _init_databases()

    _is_initializing = False


def _init_databases():
    global _task_db, _job_db, is_initialized

    try:
        _task_db = _load_database('task-db')
    except (KeyError, IOError):
        if not _is_initializing:
            raise

    try:
        _job_db = _load_database('job-db')
    except IOError:
        if not _is_initializing:
            raise
    except KeyError:
        # job database not explicitly configured
        _job_db = _task_db

    is_initialized = True


def _reset_init():
    global is_initialized
    is_initialized = (_task_db is not None and
                      _job_db is not None and
                      _config is not None)


def _load_database(name):
    cfg = _config.section(name)

    try:
        truthy = ['1', 'true', 'yes', 'on']
        if isinstance(cfg['ssl_verification'], Number):
            verify_ssl = bool(cfg['ssl_verification'])
        else:
            verify_ssl = cfg['ssl_verification'].lower() in truthy
    except KeyError:
        verify_ssl = False

    try:
        return CouchDB(
            url=cfg['url'],
            db=cfg['database'],
            username=cfg['username'],
            password=cfg['password'],
            ssl_verification=verify_ssl,
            )
    except IOError as ex:
        raise IOError("Cannot establish connection with %s CouchDB <%s>: %s" %
                      (name, cfg['url'], str(ex)))
