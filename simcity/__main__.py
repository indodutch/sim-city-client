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
SIM-CITY scripts
"""

from __future__ import print_function
import simcity
from simcity import (PrioritizedViewIterator, TaskViewIterator,
                     EndlessViewIterator, Config, FileConfig,
                     load_config_database)
import argparse
import getpass
import couchdb
import sys
import json
import signal
import traceback
from uuid import uuid4

task_views = frozenset(['pending', 'done', 'in_progress', 'error'])
job_views = frozenset(['pending_jobs', 'running_jobs', 'finished_jobs',
                       'archived_jobs', 'active_jobs'])


def main():
    """ Parse all arguments of the simcity script. """
    global task_views, job_views
    parser = argparse.ArgumentParser(prog='simcity',
                                     description='SIM-CITY scripts')

    parser.add_argument(
        '-c', '--config', help="configuration file")
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {0}\\nPython version {1}'
                        .format(simcity.__version__, sys.version))

    subparsers = parser.add_subparsers()

    cancel_parser = subparsers.add_parser('cancel', help="Cancel running job")
    cancel_parser.add_argument('job_id', help="JOB ID to cancel")
    cancel_parser.set_defaults(func=cancel)

    create_parser = subparsers.add_parser(
        'create', help="create new tasks in the database")
    create_parser.add_argument('command', help="command to run")
    create_parser.add_argument('arguments', nargs='*', help="arguments")
    create_parser.add_argument(
        '-n', '--number', type=int, default=1,
        help="number of tasks to create (default: %(default)s)")
    create_parser.add_argument(
        '-p', '--parallelism', default=1,
        help="number of threads the task needs. Use '*' for as many as "
             "available. (default: %(default)s)", )
    create_parser.add_argument(
        '-i', '--input', help="input json file")
    create_parser.set_defaults(func=create)

    delete_parser = subparsers.add_parser(
        'delete', help="Remove all documents in a view")
    delete_parser.add_argument(
        'view', help="View to remove documents from (usually one of {0})"
        .format(task_views | job_views))
    delete_parser.add_argument(
        '-d', '--design', help="design document in CouchDB", default='Monitor')
    delete_parser.set_defaults(func=delete)

    init_parser = subparsers.add_parser(
        'init', help="Initialize the SIM-CITY databases and views as "
                     "configured.")
    init_parser.add_argument(
        '-p', '--password', help="admin password")
    init_parser.add_argument(
        '-v', '--view', action='store_true',
        help="ONLY set the database views")
    init_parser.add_argument(
        '-u', '--user', help="admin user")
    init_parser.set_defaults(func=init)

    run_parser = subparsers.add_parser('run', help="Execute tasks")
    run_parser.add_argument('-D', '--days', type=int, default=1,
                            help="number of days to execute tasks "
                                 "(default: %(default)s)")
    run_parser.add_argument('-H', '--hours', type=int, default=0,
                            help="number of hours to execute tasks "
                                 "(default: %(default)s)")
    run_parser.add_argument('-M', '--minutes', type=int, default=0,
                            help="number of minutes to execute task s"
                                 "(default: %(default)s)")
    run_parser.add_argument('-S', '--seconds', type=int, default=0,
                            help="number of seconds to execute tasks "
                                 "(default: %(default)s)")
    run_parser.add_argument('-m', '--margin', type=float, default=1.5,
                            help="margin factor for average task time in "
                                 "calculating maximum time (default: "
                                 "%(default)s)")
    run_parser.add_argument('-l', '--local', action='store_true',
                            help="run locally without a job_id")
    run_parser.add_argument('-p', '--parallelism', default='*',
                            help="number of parallel processes to use for the "
                                 "computation. Use '*' for all cpu cores. "
                                 "(default: %(default)s)")
    run_parser.add_argument('-e', '--endless', action="store_true",
                            help="run until cancelled, even if no "
                                 "new jobs arrive.")
    run_parser.add_argument('-P', '--prioritize', action="store_true",
                            help="prioritize tasks")
    run_parser.add_argument('job_id', nargs='?', help="JOB ID to assume")
    run_parser.set_defaults(func=run)

    scrub_parser = subparsers.add_parser('scrub',
                                         help="Make old in progress tasks"
                                         "available for processing again")
    scrub_task_views = task_views - frozenset(['done'])
    scrub_job_views = job_views - frozenset(['archived_jobs'])
    scrub_parser.add_argument('-D', '--days', type=int, default=0,
                              help="number of days ago the task was in "
                                   "progress (default: %(default)s)")
    scrub_parser.add_argument('-H', '--hours', type=int, default=0,
                              help="number of hours ago the task was in "
                                   "progress (default: %(default)s)")
    scrub_parser.add_argument('-M', '--minutes', type=int, default=0,
                              help="number of minutes ago the task was in "
                                   "progress (default: %(default)s)")
    scrub_parser.add_argument('-S', '--seconds', type=int, default=0,
                              help="number of seconds ago the task was in "
                                   "progress (default: %(default)s)")
    scrub_parser.add_argument('view', default='in_progress',
                              help="view to scrub (default: %(default)s)",
                              choices=scrub_task_views | scrub_job_views)
    scrub_parser.set_defaults(func=scrub)

    submit_parser = subparsers.add_parser('submit', help="start a job")
    submit_parser.add_argument('host', help="host to run pilot job on")
    submit_parser.add_argument('args', nargs='*', help="command arguments")
    submit_parser.add_argument(
        '-m', '--max', type=int, default=2,
        help="only run if there are less than MAX jobs running "
             "(default: %(default)s)")
    submit_parser.add_argument(
        '-f', '--force', action='store_true',
        help="also start if there are more that MAX jobs running")
    submit_parser.set_defaults(func=submit)

    args = parser.parse_args()
    args.func(args)


def cancel(args):
    """ Cancel running job. """
    job = simcity.get_job(args.job_id)
    simcity.cancel_endless_job(job)


def create(args):
    """
    Create tasks with a single command
    """
    simcity.init(config=args.config)

    # Load the tasks to the database
    for i in range(args.number):
        try:
            task = {
                'command': args.command,
                'arguments': args.arguments,
                'parallelism': args.parallelism,
            }
            try:
                with open(args.input) as f:
                    task['input'] = json.load(f)
            except TypeError:
                pass

            simcity.add_task(task)

            print("added task %d" % i)
        except Exception as ex:
            print("ERROR: task {0} failed to be added: {1}".format(i, ex),
                  file=sys.stderr)


def delete(args):
    """ Delete all documents from given view. """
    simcity.init(config=args.config)

    if args.view in job_views:
        db = simcity.get_job_database()
    else:
        db = simcity.get_task_database()

    is_deleted = db.delete_from_view(args.view, design_doc=args.design)
    print("Deleted %d out of %d tasks from view %s" %
          (sum(is_deleted), len(is_deleted), args.view))


def init(args):
    """
    Create the databases and views
    """
    if args.user is not None and args.password is None:
        try:
            args.password = getpass.getpass('Password:')
        except KeyboardInterrupt:  # cancel password prompt
            print("")
            sys.exit(1)

    if args.view:
        config = Config()
        config.configurators.append(FileConfig(args.config))
        try:
            config.configurators.append(load_config_database(config))
        except KeyError:
            pass

        if args.user is not None:
            config.add_section('task-db', {
                'username': args.user,
                'password': args.password,
            })
            if 'job-db' in config.sections():
                config.add_section('job-db', {
                    'username': args.user,
                    'password': args.password,
                })
        try:
            simcity.init(config)
            simcity.create_views()
        except couchdb.http.ResourceNotFound:
            print("Database not initialized, run `simcity init` without -v "
                  "flag.")
            sys.exit(1)
        except couchdb.http.Unauthorized:
            print("CouchDB user and/or password incorrect")
            sys.exit(1)
    else:
        try:
            simcity.init(config=args.config)
        except couchdb.http.ResourceNotFound:
            pass  # database does not exist yet
        except couchdb.http.Unauthorized:
            pass  # user does not exist yet

        try:
            simcity.create(args.admin, args.password)
        except couchdb.http.Unauthorized:
            print("User and/or password incorrect")
            sys.exit(1)


def _is_cancelled():
    """ Whether the job was cancelled """
    db = simcity.get_job_database()
    try:
        job_id = simcity.get_current_job_id()
        return db.get(job_id)['cancel'] > 0
    except KeyError:
        return False


def _signal_handler(signal, frame):
    """ Catch signals to do a proper cleanup.
        The job then has time to write out any results or errors. """
    print('Caught signal %d; finishing job.' % signal, file=sys.stderr)
    try:
        simcity.finish_job(simcity.get_job())
    except Exception as ex:
        print('Failed during clean-up: {0}'.format(ex))

    sys.exit(1)


def _time_args_to_seconds(args):
    """
    Convert an object with days, hours, minutes and seconds properties
    to a single seconds int.
    """
    hours = args.hours + (24 * args.days)
    return args.seconds + 60 * (args.minutes + 60 * hours)


def run(args):
    """ Run job to process tasks. """
    simcity.init(config=args.config)

    if args.job_id is not None:
        simcity.set_current_job_id(args.job_id)
    elif args.local:
        simcity.set_current_job_id('local-' + uuid4().hex)

    db = simcity.get_task_database()

    if args.prioritize:
        iterator = PrioritizedViewIterator(db, 'pending_priority', 'pending')
    else:
        iterator = TaskViewIterator(db, 'pending')

    if args.endless:
        iterator = EndlessViewIterator(iterator, stop_callback=_is_cancelled)

    actor = simcity.JobActor(iterator, simcity.ExecuteWorker)

    for sig_name in ['HUP', 'INT', 'QUIT', 'ABRT', 'TERM']:
        try:
            sig = signal.__dict__['SIG%s' % sig_name]
        except Exception as ex:
            print(ex, file=sys.stderr)
        else:
            signal.signal(sig, _signal_handler)

    # Start work!
    print("Connected to the database sucessfully. Now starting work...")
    try:
        actor.run(maxtime=_time_args_to_seconds(args),
                  avg_time_factor=args.margin)
    except Exception as ex:
        print("Error occurred: %s: %s" % (str(type(ex)), str(ex)),
              file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

    print("No more tasks to process, done.")


def scrub(args):
    """
    Scrub tasks or jobs in a given view to return to their previous status.
    """
    global task_views
    simcity.init(config=args.config)

    age = _time_args_to_seconds(args)

    if args.view in task_views:
        scrubbed, total = simcity.scrub_tasks(args.view, age=age)
    else:
        scrubbed, total = simcity.scrub_jobs(args.view, age=age)

    if scrubbed > 0:
        print("Scrubbed %d out of %d documents from '%s'" %
              (scrubbed, total, args.view))
    else:
        print("No scrubbing required")


def submit(args):
    """ Submit job to the infrastructure """
    simcity.init(config=args.config)

    if args.force:
        job = simcity.submit(args.host)
    else:
        job = simcity.submit_if_needed(args.host, args.max)
    if job is None:
        print("No tasks to process or already %d jobs running (increase "
              "maximum number of jobs with -m)" % args.max)
    else:
        print("Job %s (ID: %s) started" % (job['batch_id'], job.id))
