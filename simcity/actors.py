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

""" Actors execute tasks from the database and upload the results. """

from __future__ import print_function

import simcity
from .util import listfiles, write_json, expandfilename
from .task import upload_attachment
from picas.util import Timer
from couchdb.http import ResourceConflict
try:
    from Queue import Empty as QueueEmpty
except ImportError:
    from queue import Empty as QueueEmpty
from multiprocessing import cpu_count, Process, Manager

import os
from subprocess import call


class Worker(Process):
    """
    Worker class, override the process_task method.
    @param config: config object
    @param number: worker number
    @param task_q: (incoming) task queue, with None as sentinel value
    @param result_q: (outgoing) result queue
    @param config: config object
    """
    def __init__(self, number, config, task_q, result_q, queued_semaphore,
                 *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.number = number
        self.config = config
        self.task_q = task_q
        self.result_q = result_q
        self.queued_semaphore = queued_semaphore

    def process_task(self, task):
        """
        Processes a single task. Will modify the task in place. Override in
        subclass.
        @param task: picas.Task object to modify
        """
        raise NotImplementedError

    def run(self):
        """
        Start a new worker instance
        """
        print('Starting worker {0}'.format(self.number))
        try:
            for task in iter(self.task_q.get, None):
                try:
                    self.process_task(task)
                except Exception as ex:
                    msg = ("Exception {0} occurred during processing: {1}"
                           .format(type(ex), ex))
                    task.error(msg, exception=ex)
                    print(msg)
                finally:
                    for _ in range(task['parallelism']):
                        self.queued_semaphore.release()

                    self.result_q.put(task)
            print('Ending worker {0}'.format(self.number))
        except Exception as ex:
            print("Worker {0} failed: {1}".format(self.number, ex))
        finally:
            try:
                self.result_q.put(None)
            except IOError:
                pass


class ExecuteWorker(Worker):
    """
    Executes a job locally, all tasks provided by its iterator.

    Tasks are assumed to have input parameters. It creates a new input, output
    and temporary directory for each tasks, and attaches files that are
    generated in the output directory to the task when the task is finished.

    If the command exits with non-zero, the task is assumed to have failed.

    At the start of a job, it is registered to the job database, when it is
    finished, it is archived.
    """
    def __init__(self, *args, **kwargs):
        super(ExecuteWorker, self).__init__(*args, **kwargs)

    def process_task(self, task):
        """ Processes a single task from the database by executing it.

        First, input, output and temporary directories are created. Then the
        input is written to the file [in_dir]/input.json. The file is executed
        and all output files generated in [out_dir] are uploaded. This includes
        the stdout and stderr of the execution.
        """
        print("-----------------------")
        print("Working on task: {0}".format(task.id))

        dirs = self.create_dirs(task)
        params_file = os.path.join(dirs['SIMCITY_IN'], 'input.json')
        dirs['SIMCITY_PARAMS'] = params_file

        write_json(params_file, task.input)
        command = expandfilename(task['command'])

        if 'arguments' in task and len(task['arguments']) > 0:
            command = [command]
            for arg in task['arguments']:
                if arg in dirs:
                    command.append(dirs[arg])
                elif arg.startswith('$') and arg[1:] in dirs:
                    command.append(dirs[arg[1:]])
                else:
                    command.append(arg)

        task['execute_properties'] = {'env': dirs}

        out_file = os.path.join(dirs['SIMCITY_OUT'], 'stdout.txt')
        err_file = os.path.join(dirs['SIMCITY_OUT'], 'stderr.txt')
        try:
            if self.execute(command, out_file, err_file, dirs) != 0:
                task.error("Command failed")
        except Exception as ex:
            task.error("Command raised exception", ex)

        task.output = {}

        # Read all files in as attachments
        out_files = listfiles(dirs['SIMCITY_OUT'])
        for filename in out_files:
            upload_attachment(task, dirs['SIMCITY_OUT'], filename)

        if not task.has_error():  # don't override error status
            task.done()
        print("-----------------------")

    @staticmethod
    def execute(command, out_file, err_file, env):
        """ Execute command and write the stdout and stderr to given
        filenames. """
        with open(out_file, 'w') as out:
            with open(err_file, 'w') as err:
                return call(command, env=env, stdout=out, stderr=err)

    def create_dirs(self, task):
        """ Create the directories to read and store data. """
        dir_map = {
            'SIMCITY_TMP': 'tmp_dir',
            'SIMCITY_IN':  'input_dir',
            'SIMCITY_OUT': 'output_dir'
        }

        dirs = {}
        for d, conf in dir_map.items():
            super_dir = expandfilename(self.config[conf])
            try:
                os.mkdir(super_dir)
            except OSError:  # directory exists
                pass

            dirs[d] = os.path.join(super_dir,
                                   task.id + '_' + str(task['lock']))
            os.mkdir(dirs[d])

        return dirs


class JobActor(object):
    """
    Executes tasks as a single job with multiple processes
    """
    def __init__(self, iterator, worker_cls, task_db=None, parallelism=None,
                 job_db=None, config=None):
        """
        @param iterator: the iterator to get the tasks from.
        """
        self.iterator = iterator
        self.worker_cls = worker_cls

        if task_db is None:
            task_db = simcity.get_task_database()
        self.task_db = task_db

        if job_db is None:
            job_db = simcity.get_job_database()
        self.job_db = job_db

        if config is None:
            config = simcity.get_config()
        self.config = config.section('Execution')

        if parallelism is None:
            parallelism = self.config.get('parallelism', 1)

        if parallelism == '*':
            self.parallelism = cpu_count()
        else:
            self.parallelism = min(cpu_count(), int(parallelism))

        self.manager = Manager()
        self.task_q = self.manager.Queue()
        self.result_q = self.manager.Queue()
        self.queued_semaphore = self.manager.Semaphore(self.parallelism)
        self.workers = [worker_cls(i, self.config, self.task_q, self.result_q,
                                   self.queued_semaphore)
                        for i in range(self.parallelism)]

        self.tasks_processed = self.manager.Value('i', 0)
        self.job = None
        self.collector = CollectActor(
            self.task_db, self.parallelism, self.result_q,
            self.tasks_processed)

    def run(self, maxtime=None, avg_time_factor=0.0):
        """Run method of the actor, executes the application code by iterating
        over the available tasks in CouchDB.
        """
        time = Timer()
        self.prepare_env()
        self.collector.start()

        for w in self.workers:
            w.start()
        try:
            for task in self.iterator:
                self.set_task_parallelism(task)

                for _ in range(task['parallelism']):
                    self.queued_semaphore.acquire()

                processed = self.tasks_processed.value
                if maxtime is not None and processed > 0:
                    will_elapse = ((avg_time_factor + processed) *
                                   time.elapsed() / processed)
                    if will_elapse > maxtime:
                        break

                self.task_q.put(task)

            for _ in range(self.parallelism):
                self.queued_semaphore.acquire()
        finally:
            self.cleanup_env()

    def set_task_parallelism(self, task):
        """ Determine the preferred parallelism of a task and set it
        in the parallelism property. """
        if 'parallelism' not in task:
            task['parallelism'] = 1
        elif task['parallelism'] == '*':
            task['parallelism'] = self.parallelism
        else:
            task['parallelism'] = min(int(task['parallelism']),
                                      self.parallelism)

    def prepare_env(self):
        """ Prepares the current job by registering it as started in the
        database. """
        self.job = simcity.start_job(
            database=self.job_db, properties={'parallelism': self.parallelism})

    def cleanup_env(self):
        """ Cleans up the current job by registering it as finished. """
        try:
            for _ in self.workers:
                self.task_q.put(None)
        except IOError:
            pass

        for w in self.workers:
            w.join()
        self.collector.join()

        self.job['tasks_processed'] = self.tasks_processed.value
        simcity.finish_job(self.job, database=self.job_db)


class CollectActor(Process):
    def __init__(self, task_db, parallelism, result_q, tasks_processed):
        super(CollectActor, self).__init__()
        self.result_q = result_q
        self.task_db = task_db
        self.is_done = False
        self.tasks_processed = tasks_processed
        self.parallelism = parallelism
        self.workers_done = 0

    def run(self):
        self.task_db = self.task_db.copy()

        while self.workers_done < self.parallelism:
            try:
                task = self.result_q.get()
                if task is None:
                    self.workers_done += 1
                    continue

                save_task(task, self.task_db)
                self.tasks_processed.value += 1
            except QueueEmpty:
                pass
            except EOFError:
                self.workers_done = self.parallelism


def save_task(task, task_db):
    """ Save task to database. """
    saved = False
    while not saved:
        try:
            print('saving {0}'.format(task.doc))
            task_db.save(task)
            saved = True
        except ResourceConflict:
            # simply overwrite changes - model results are more
            # important
            new_task = task_db.get(task.id)
            task['_rev'] = new_task.rev
