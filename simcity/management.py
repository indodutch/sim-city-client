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
Initialize SIM-CITY variables and databases based on the SIM-CITY configuration
file.
"""

import picas
from .util import Config, get_truthy
import couchdb
from couchdb.http import ResourceNotFound, Unauthorized
import pystache
import easywebdav
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import os

try:
    _current_job_id = os.environ['SIMCITY_JOBID']
except:
    _current_job_id = None

_config = None
is_initialized = False
_is_initializing = True
_task_db = None
_job_db = None
_webdav = {}


def _reset_globals():
    """ Set all globals to their default values. """
    global _config, _task_db, _job_db, _webdav, is_initialized
    global _webdav
    _config = None
    is_initialized = False
    _task_db = None
    _job_db = None
    _webdav = {}


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


def uses_webdav():
    """ Whether simcity is configured for webdav. """
    _check_init(_config)
    try:
        dav_cfg = _config.section('webdav')
    except KeyError:
        return False
    else:
        return ('url' in dav_cfg and
                get_truthy(dav_cfg.get('enabled', True)))


def get_webdav(process=None):
    """ Gets or creates a webdav connection.

    Connections cannot be shared between processes, so provide a process ID if
    multiprocessing/threading is used. """
    if process not in _webdav:
        if not uses_webdav():
            raise EnvironmentError("Webdav is not configured")
        dav_cfg = _config.section('webdav')

        url = urlparse(dav_cfg['url'])
        verify = get_truthy(dav_cfg.get('ssl_verification', True))
        if verify and 'certificate' in dav_cfg:
            verify = dav_cfg['certificate']
        _webdav[process] = easywebdav.connect(
            host=url.hostname,
            protocol=url.scheme,
            port=url.port,
            path=url.path[1:],
            auth=(dav_cfg['username'], dav_cfg['password']),
            verify_ssl=verify)

    return _webdav[process]


def _check_init(myvalue=None):
    """ Check whether simcity is initialized.

    It checks a single value, such as _task_db or _config for null.
    Raises an EnvironmentError if not initialized.
    """
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


def create(admin_user, admin_password):
    """ Create a SIM-CITY infrastructure.

    Tries to load CouchDB databases and creates users, databases and views that
    are not yet configured.
    param admin_user:
        user on the CouchDB databases that is allowed to create users and
        databases
    param admin_password:
        password of the admin user
    """
    global _task_db, _job_db

    taskcfg = _config.section('task-db')
    try:
        _create_user(taskcfg, admin_user, admin_password)
        print("Created database user %s in CouchDB %s" %
              (taskcfg['username'], taskcfg['url']))
    except couchdb.http.ResourceConflict:
        print("User %s exists in CouchDB %s" %
              (taskcfg['username'], taskcfg['url']))

    try:
        _task_db = _load_database('task-db', admin_user, admin_password)
        print("Created task database %s at URL %s" %
              (taskcfg['database'], taskcfg['url']))
        taskdb_existed = False
        usernames = [taskcfg['username']]
        _task_db.set_users(admins=usernames, members=usernames)
        print("Added user %s to task database %s" %
              (taskcfg['username'], taskcfg['database']))
    except couchdb.http.PreconditionFailed:
        _task_db = _load_database('task-db')
        print("Loaded existing task database %s at URL %s" %
              (taskcfg['database'], taskcfg['url']))
        taskdb_existed = True

    try:
        jobcfg = _config.section('job-db')
        same_connection = (
            jobcfg['url'] == taskcfg['url'] and
            jobcfg['username'] == taskcfg['username'])
        same_db = (
            jobcfg['url'] == taskcfg['url'] and
            jobcfg['database'] == taskcfg['database'])

        if not same_connection:
            try:
                _create_user(jobcfg, admin_user, admin_password)
                print("Created database user %s in CouchDB %s" %
                      (jobcfg['username'], jobcfg['url']))
            except couchdb.http.ResourceConflict:
                print("User %s exists in CouchDB %s" %
                      (jobcfg['username'], jobcfg['url']))

        if same_db:
            print("Using shared task/job database")
            _job_db = _task_db
            if (not taskdb_existed and
                    jobcfg['username'] != taskcfg['username']):
                usernames = [taskcfg['username'], jobcfg['username']]
                _job_db.set_users(admins=usernames, members=usernames)
                print("Added user %s to shared task/job database %s" %
                      (jobcfg['username'], jobcfg['database']))
        else:
            try:
                _job_db = _load_database('job-db', admin_user, admin_password)
                print("Created job database %s in CouchDB %s" %
                      (jobcfg['database'], jobcfg['url']))
                usernames = [jobcfg['username']]
                _job_db.set_users(admins=usernames, members=usernames)
                print("Added user %s to job database %s" %
                      (jobcfg['username'], jobcfg['database']))
            except couchdb.http.PreconditionFailed:
                print("Loaded existing job database %s from CouchDB %s" %
                      (jobcfg['database'], jobcfg['url']))
    except KeyError:
        pass

    _init_databases()
    create_views()
    print("Created views in databases")


def _create_user(cfg, admin_user, admin_password):
    """ Create a new user in the CouchDB database referenced by cfg.
        The admin_user and admin_password are needs to have permission to
        create a user."""
    verification = get_truthy(cfg.get('ssl_verification', False))
    users = picas.CouchDB(
        url=cfg['url'],
        db='_users',
        username=admin_user,
        password=admin_password,
        ssl_verification=verification)
    users.save(picas.User(cfg['username'], cfg['password']))


def create_views():
    '''
    Create views necessary to run simcity client with.
    '''
    taskMapTemplate = '''
    function(doc) {
      if(doc.type === 'task' && {{condition}}) {
        emit(doc._id, {
            lock: doc.lock,
            done: doc.done,
        });
      }
    }
        '''
    erroneousMapCode = '''
    function(doc) {
      if (doc.type === 'task' && doc.lock == -1) {
        emit(doc._id, doc.error);
      }
    }
        '''
    jobMapTemplate = '''
    function(doc) {
      if (doc.type == 'job' && {{condition}}) {
        emit(doc._id, {
            queue: doc.queue,
            start: doc.start,
            done: doc.done,
        });
      }
    }
        '''
    overviewMapTemplate = '''
    function(doc) {
      if(doc.type === 'task') {
      {{#tasks}}
        if ({{condition}}) {
          emit("{{name}}", 1);
        }
      {{/tasks}}
        if (doc.lock === -1) {
          emit("error", 1);
        }
      }
      if (doc.type === 'job') {
      {{#jobs}}
        if ({{condition}}) {
          emit('{{name}}', 1);
        }
      {{/jobs}}
      }
    }
    '''
    overviewReduceCode = '''
    function (key, values, rereduce) {
       return sum(values);
    }
    '''

    tasks = {
        'todo':     'doc.lock === 0',
        'todo_priority': 'doc.lock === 0 && doc.priority === "high"',
        'locked':   'doc.lock > 0 && doc.done === 0',
        'done':     'doc.lock > 0 && doc.done > 0'
    }
    jobs = {
        'pending_jobs':  'doc.start === 0 && doc.archive === 0',
        'active_jobs':  'doc.start > 0 && doc.done == 0 && doc.archive === 0',
        'finished_jobs': 'doc.done > 0',
        'archived_jobs': 'doc.archive > 0'
    }
    pystache_views = {
        'tasks': [{'name': view, 'condition': condition}
                  for view, condition in tasks.items()],
        'jobs':  [{'name': view, 'condition': condition}
                  for view, condition in jobs.items()]
    }
    renderer = pystache.renderer.Renderer(escape=lambda u: u)

    for view in pystache_views['tasks']:
        mapCode = renderer.render(taskMapTemplate, view)
        _task_db.add_view(view['name'], mapCode)

    for view in pystache_views['jobs']:
        mapCode = renderer.render(jobMapTemplate, view)
        _job_db.add_view(view['name'], mapCode)

    _task_db.add_view('error', erroneousMapCode)

    # overview_total View -- lists all views and the number of tasks in each
    # view
    overviewMapCode = renderer.render(overviewMapTemplate, pystache_views)
    _task_db.add_view('overview_total', overviewMapCode, overviewReduceCode)
    _job_db.add_view('overview_total', overviewMapCode, overviewReduceCode)


def _init_databases():
    """ Connect to the databases defined in the configuration file. """
    global _task_db, _job_db, is_initialized

    try:
        _task_db = _load_database('task-db')
    except (KeyError, IOError, ResourceNotFound, Unauthorized):
        if not _is_initializing:
            raise

    try:
        taskcfg = _config.section('task-db')
        jobcfg = _config.section('job-db')
        if (jobcfg['url'] == taskcfg['url'] and
                jobcfg['database'] == taskcfg['database'] and
                jobcfg['username'] == taskcfg['username']):
            _job_db = _task_db
        else:
            _job_db = _load_database('job-db')
    except (IOError, ResourceNotFound, Unauthorized):
        if not _is_initializing:
            raise
    except KeyError:
        # job database not explicitly configured
        _job_db = _task_db

    is_initialized = True


def _reset_init():
    """ Check again whether simcity is properly initialized. """
    global is_initialized
    is_initialized = (_task_db is not None and
                      _job_db is not None and
                      _config is not None)


def _load_database(name, admin_user=None, admin_password=""):
    """ Load a database from configuration.

    If it does not exist and the admin credentials are provided, the database
    will be created. """
    cfg = _config.section(name)

    try:
        if admin_user is None:
            user, password, create = cfg['username'], cfg['password'], False
        else:
            user, password, create = admin_user, admin_password, True

        return picas.CouchDB(
            url=cfg['url'],
            db=cfg['database'],
            username=user,
            password=password,
            ssl_verification=get_truthy(cfg.get('ssl_verification', False)),
            create=create)
    except IOError as ex:
        raise IOError("Cannot establish connection with %s CouchDB <%s>: %s" %
                      (name, cfg['url'], str(ex)))
