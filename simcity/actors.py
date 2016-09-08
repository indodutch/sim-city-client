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

from .util import Timer
from couchdb.http import ResourceConflict
try:
    from Queue import Empty as QueueEmpty
except ImportError:
    from queue import Empty as QueueEmpty
from multiprocessing import cpu_count, Process, Manager


class JobActor(object):
    """
    Executes tasks as a single job with multiple processes
    """
    def __init__(self, iterator, worker_cls, config, task_db, job_handler,
                 attachments, parallelism=None):
        """
        @param iterator: the iterator to get the tasks from.
        """
        self.iterator = iterator
        self.worker_cls = worker_cls

        self.task_db = task_db
        self.job_handler = job_handler
        self.config = config.section('Execution')
        self.attachments = attachments

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
                                   self.queued_semaphore, self.attachments)
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
        self.job = self.job_handler.start(
            properties={'parallelism': self.parallelism})

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

        self.job_handler.finish(self.job, self.tasks_processed.value)


class CollectActor(Process):
    """ Collects finished tasks from the JobActor """
    def __init__(self, task_db, parallelism, result_q, tasks_processed):
        super(CollectActor, self).__init__()
        self.result_q = result_q
        self.task_db = task_db
        self.is_done = False
        self.tasks_processed = tasks_processed
        self.parallelism = parallelism
        self.workers_done = 0

    def run(self):
        """ In the new process, create a new database connection and put
        finished jobs there. """
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
            task_db.save(task)
            saved = True
        except ResourceConflict:
            # simply overwrite changes - model results are more
            # important
            new_task = task_db.get(task.id)
            task['_rev'] = new_task.rev
