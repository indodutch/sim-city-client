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
from .task import TaskHandler
from .job import JobHandler
from .util import get_truthy, seconds
from .config import Config, FileConfig, CouchDBConfig
from .database import CouchDB
from .document import User, Task, Job
from .dav import (RestRequests, Attachments, CouchAttachmentHandler,
                  RestAttachmentHandler)
from .submit import SubmitHandler, SubmitAdaptor
import couchdb
import pystache
import time
import os

try:
    _current_job_id = os.environ['SIMCITY_JOBID']
except KeyError:
    _current_job_id = None


class Barbecue(object):
    def __init__(self, config, job_id=None, task_db=None, job_db=None,
                 config_db=None, webdav=None):
        if isinstance(config, Config):
            self.config = config
        else:
            self.config = load_config(config)

        self._current_job_id = job_id

        if task_db is not None:
            self._task_db = task_db
        else:
            self._task_db = load_database('task-db', self.config)

        try:
            task_cfg = self.config.section('task-db')
            job_cfg = self.config.section('job-db')
            if (job_cfg['url'] == task_cfg['url'] and
                    job_cfg['database'] == task_cfg['database'] and
                    job_cfg.get('username') == task_cfg.get('username')):
                self._job_db = self._task_db
            else:
                self._job_db = load_database('job-db', self.config)
        except KeyError:
            # job database not explicitly configured
            self._job_db = self._task_db

        self.attachments = Attachments()
        try:
            dav_cfg = self.config.section('webdav')
        except KeyError:
            pass
        else:
            if get_truthy(dav_cfg.get('enabled', True)):
                dav_cfg = self.config.section('webdav')

                if 'username' in dav_cfg:
                    auth = (dav_cfg['username'], dav_cfg['password'])
                else:
                    auth = None

                rest = RestRequests(dav_cfg['url'], auth=auth)
                self.attachments.handlers.append(RestAttachmentHandler(rest))
        self.attachments.handlers.append(CouchAttachmentHandler(self._task_db))

        self.tasks = TaskHandler(self.task_db, self.attachments)
        self.jobs = JobHandler(self.job_db, self.current_job_id)
        self.submitter = SubmitHandler(self.config, self.jobs)

    @property
    def task_db(self):
        return self._task_db

    @property
    def job_db(self):
        return self._job_db

    @property
    def current_job_id(self):
        return self._current_job_id

    def create_views(self):
        """
        Create views necessary to run simcity client with.
        """
        task_map_template = '''
        function(doc) {
          if(doc.type === 'task' && {{condition}}) {
            emit(doc._id, {
                lock: doc.lock,
                done: doc.done,
            });
          }
        }
            '''
        erroneous_map_code = '''
        function(doc) {
          if (doc.type === 'task' && doc.lock == -1) {
            emit(doc._id, doc.error);
          }
        }
            '''
        job_map_template = '''
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
        overview_map_template = '''
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
        overview_reduce_code = '''
        function (key, values, rereduce) {
           return sum(values);
        }
        '''

        tasks = {
            'pending': 'doc.lock === 0',
            'pending_priority': 'doc.lock === 0 && doc.priority === "high"',
            'in_progress': 'doc.lock > 0 && doc.done === 0',
            'done': 'doc.lock > 0 && doc.done > 0'
        }
        jobs = {
            'pending_jobs': 'doc.start === 0 && doc.archive === 0',
            'running_jobs': 'doc.start > 0 && doc.done == 0 && '
                            'doc.archive === 0',
            'finished_jobs': 'doc.done > 0',
            'archived_jobs': 'doc.archive > 0',
            'active_jobs': '!doc.archive'
        }
        pystache_views = {
            'tasks': [{'name': view, 'condition': condition}
                      for view, condition in tasks.items()],
            'jobs': [{'name': view, 'condition': condition}
                     for view, condition in jobs.items()]
        }
        renderer = pystache.Renderer(escape=lambda u: u)

        for view in pystache_views['tasks']:
            map_code = renderer.render(task_map_template, view)
            self.task_db.add_view(view['name'], map_code)

        for view in pystache_views['jobs']:
            map_code = renderer.render(job_map_template, view)
            self.job_db.add_view(view['name'], map_code)

        self.task_db.add_view('error', erroneous_map_code)

        # overview_total View -- lists all views and the number of tasks in
        # each view
        overview_map_code = renderer.render(overview_map_template,
                                            pystache_views)
        self.task_db.add_view('overview_total', overview_map_code,
                              overview_reduce_code)
        self.job_db.add_view('overview_total', overview_map_code,
                             overview_reduce_code)

    @staticmethod
    def load_config_database(config):
        """
        Load configuration database

        @raise KeyError: the database is not fully configured
        """
        try:
            url = os.environ['SIMCITY_CONFIG_URL']
            db = os.environ['SIMCITY_CONFIG_DB']
            user = os.environ.get('SIMCITY_CONFIG_USER')
            password = os.environ.get('SIMCITY_CONFIG_PASSWORD')
            return CouchDBConfig.from_url(
                url, db, user, password)
        except KeyError:
            cfg = config.section('config-db')
            return CouchDBConfig.from_url(cfg['url'], cfg['db'],
                                          cfg.get('user'), cfg.get('password'))

    @staticmethod
    def create(config, admin_user, admin_password):
        """
        Create a SIM-CITY infrastructure.

        Tries to load CouchDB databases and creates users, databases and views that
        are not yet configured.
        param admin_user:
            user on the CouchDB databases that is allowed to create users and
            databases
        param admin_password:
            password of the admin user
        """
        task_cfg = config.section('task-db')
        try:
            _create_user(task_cfg, admin_user, admin_password)
            print("Created database user %s in CouchDB %s" %
                  (task_cfg['username'], task_cfg['url']))
        except couchdb.http.ResourceConflict:
            print("User %s exists in CouchDB %s" %
                  (task_cfg['username'], task_cfg['url']))

        try:
            task_db = load_database('task-db', config, admin_user,
                                    admin_password)
            print("Created task database %s at URL %s" %
                  (task_cfg['database'], task_cfg['url']))
            task_db_existed = False
            usernames = [task_cfg['username']]
            task_db.set_users(admins=usernames, members=usernames)
            print("Added user %s to task database %s" %
                  (task_cfg['username'], task_cfg['database']))
        except couchdb.http.PreconditionFailed:
            task_db = load_database('task-db', config)
            print("Loaded existing task database %s at URL %s" %
                  (task_cfg['database'], task_cfg['url']))
            task_db_existed = True

        try:
            job_cfg = config.section('job-db')
            same_connection = (
                job_cfg['url'] == task_cfg['url'] and
                job_cfg['username'] == task_cfg['username'])
            same_db = (
                job_cfg['url'] == task_cfg['url'] and
                job_cfg['database'] == task_cfg['database'])

            if not same_connection:
                try:
                    _create_user(job_cfg, admin_user, admin_password)
                    print("Created database user %s in CouchDB %s" %
                          (job_cfg['username'], job_cfg['url']))
                except couchdb.http.ResourceConflict:
                    print("User %s exists in CouchDB %s" %
                          (job_cfg['username'], job_cfg['url']))

            if same_db:
                print("Using shared task/job database")
                job_db = task_db
                if (not task_db_existed and
                            job_cfg['username'] != task_cfg['username']):
                    usernames = [task_cfg['username'], job_cfg['username']]
                    job_db.set_users(admins=usernames, members=usernames)
                    print("Added user %s to shared task/job database %s" %
                          (job_cfg['username'], job_cfg['database']))
            else:
                try:
                    job_db = load_database('job-db', config, admin_user,
                                                 admin_password)
                    print("Created job database %s in CouchDB %s" %
                          (job_cfg['database'], job_cfg['url']))
                    usernames = [task_cfg['username']]
                    job_db.set_users(admins=usernames, members=usernames)
                    print("Added user %s to job database %s" %
                          (job_cfg['username'], job_cfg['database']))
                except couchdb.http.PreconditionFailed:
                    print("Loaded existing job database %s from CouchDB %s" %
                          (job_cfg['database'], job_cfg['url']))
        except KeyError:
            pass

    def run_task(self, task_properties, host, max_jobs, polling_time=None):
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
        task = self.tasks.add(task_properties)
        job = self.submit_if_needed(host, max_jobs)

        if polling_time is not None:
            while task['done'] == 0:
                time.sleep(polling_time)
                task = self.tasks.get(task.id)

        return task, job

    def overview_total(self):
        """
        Overview of all tasks and jobs.

        Returns a dict with the numbers of each type of job and task.
        """
        views = ['pending', 'in_progress', 'error', 'done',
                 'finished_jobs', 'running_jobs', 'pending_jobs']
        num = dict((view, 0) for view in views)

        for view in self.task_db.view('overview_total', group=True):
            num[view.key] = view.value

        if self.task_db is not self.job_db:
            for view in self.job_db.view('overview_total', group=True):
                num[view.key] = view.value

        return num

    def submit_if_needed(self, host_id, max_jobs):
        """
        Submit a new job if not enough jobs are already running or queued.

        Host configuration is extracted from an entry in the global config file.
        """
        num = self.overview_total()

        num_jobs = num['running_jobs'] + num['pending_jobs']
        num_tasks = num['pending'] + num['in_progress']
        if num_jobs < min(num_tasks, max_jobs):
            adaptor = self.submitter.adaptor(host_id=host_id)
            return adaptor.submit()
        else:
            return None

    def submit_while_needed(self, host_id, max_jobs, dry_run=False):
        """
        Submits new job while not enough jobs are already running or queued.

        Host configuration is extracted from an entry in the global config file.
        """
        num = self.overview_total()
        num_jobs = num['running_jobs'] + num['pending_jobs']
        num_tasks = num['pending'] + num['in_progress']

        new_jobs = max(0, min(num_tasks, max_jobs) - num_jobs)

        if dry_run:
            return [None] * new_jobs
        else:
            adaptor = self.submitter.adaptor(host_id=host_id)
            return [adaptor.submit() for _ in range(new_jobs)]

    def check_job_status(self, dry_run=False):
        """
        Check the current job status of jobs that the database considers active.
        If dry_run is false, modify incongruent job statuses.
        @param dry_run: do not modify job
        @param database: job database
        @return: list of jobs that are archived
        """
        jobs = [self.jobs.get(row.id)
                for row in self.job_db.view('active_jobs')]
        jobs = [job for job in jobs if job.type == 'job']
        job_status = self.submitter.status(jobs)

        new_jobs = []
        five_days = 5 * 24 * 60 * 60
        for stat, job in zip(job_status, jobs):
            if ((stat is None and seconds() - job['queue'] > five_days) or
                    stat == SubmitAdaptor.DONE):
                if dry_run:
                    new_jobs.append(job)
                else:
                    new_jobs.append(self.jobs.archive(job))

        return new_jobs

    def check_task_status(self, dry_run=False):
        """
        Check the current task status of in_progress tasks  against the job status
        of the job that it is supposed to be executing it.
        If dry_run is false, modify incongruent task statuses.
        @param dry_run: do not modify task
        @param database: task database
        @return: list of tasks that are marked as in error because their job is not
            running
        """
        has_failed_saves = True
        new_tasks = []
        while has_failed_saves:
            has_failed_saves = False

            for row in self.task_db.view('in_progress'):
                task = self.tasks.get(row.id)
                job = self.jobs.get(task['job'])
                if job.is_done():
                    if dry_run:
                        new_tasks.append(task)
                    else:
                        try:
                            new_tasks.append(self.task_db.save(task))
                        except couchdb.http.ResourceConflict:
                            has_failed_saves = True

        return new_tasks

    def scrub(self, view, age=24 * 60 * 60, database=None):
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
            database = self.task_db if is_task else self.job_db

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


def _create_user(cfg, admin_user, admin_password):
    """ Create a new user in the CouchDB database referenced by cfg.
        The admin_user and admin_password are needs to have permission to
        create a user."""
    verification = get_truthy(cfg.get('ssl_verification', False))
    users = CouchDB(
        url=cfg['url'],
        db='_users',
        username=admin_user,
        password=admin_password,
        ssl_verification=verification)
    users.save(User(cfg['username'], cfg['password']))


def load_config(config, config_db=None):
    """
    Load configuration

    @raise KeyError: the database is not fully configured
    """
    cfg = Config()
    cfg.configurators.append(FileConfig(config))
    if config_db is not None:
        cfg.configurators.append(config_db)
    elif ('SIMCITY_CONFIG_URL' in os.environ and
                'SIMCITY_CONFIG_DB' in os.environ):
        url = os.environ['SIMCITY_CONFIG_URL']
        db = os.environ['SIMCITY_CONFIG_DB']
        user = os.environ.get('SIMCITY_CONFIG_USER')
        password = os.environ.get('SIMCITY_CONFIG_PASSWORD')
        cfg.configurators.append(CouchDBConfig.from_url(
            url, db, user, password))
    else:
        try:
            cfg_cfg = config.section('config-db')
            cfg.configurators.append(CouchDBConfig.from_url(
                cfg_cfg['url'], cfg_cfg['db'], cfg_cfg.get('user'),
                cfg_cfg.get('password')))
        except KeyError:
            pass
    return cfg


def load_webdav(config):
    dav_cfg = config.section('webdav')
    if not get_truthy(dav_cfg.get('enabled', True)):
        return None

    dav_cfg = config.section('webdav')

    if 'username' in dav_cfg:
        auth = (dav_cfg['username'], dav_cfg['password'])
    else:
        auth = None

    return RestRequests(dav_cfg['url'], auth=auth)


def load_database(name, config, admin_user=None, admin_password=""):
    """ Load a database from configuration.

    If it does not exist and the admin credentials are provided, the database
    will be created. """
    cfg = config.section(name)

    if get_truthy(cfg.get('no_database', False)):
        return None
    try:
        if admin_user is None:
            user = cfg.get('username')
            password = cfg.get('password', '')
            do_create = False
        else:
            user, password, do_create = admin_user, admin_password, True

        return CouchDB(
            url=cfg['url'],
            db=cfg['database'],
            username=user,
            password=password,
            ssl_verification=get_truthy(cfg.get('ssl_verification', False)),
            create=do_create)
    except IOError as ex:
        raise IOError("Cannot establish connection with %s CouchDB <%s>: %s" %
                      (name, cfg['url'], str(ex)))
