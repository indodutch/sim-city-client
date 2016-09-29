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

from simcity import Task
from simcity.worker import Worker, ExecuteWorker
from multiprocessing import Queue, Semaphore


class MockWorker(Worker):
    def __init__(self, *args, **kwargs):
        super(MockWorker, self).__init__(*args, **kwargs)

    def process_task(self, task):
        task.output['myoutput'] = 1

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.task_q.put(None)
        self.join()


def test_worker_not_implemented():
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=1)
    task = Task({'something': 'anything', 'parallelism': 1})
    w = Worker(1, {}, task_q, result_q, semaphore)
    task_q.put(task)
    task_q.put(None)
    w.run()
    result_task = result_q.get()
    assert task.id == result_task.id
    errors = result_task.get_errors()
    print(errors)
    assert len(errors) == 1
    assert 'NotImplementedError' in errors[0]['exception']
    assert result_q.get() is None


def test_worker_mp_not_implemented():
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=1)
    task = Task({'something': 'anything', 'parallelism': 1})
    w = Worker(1, {}, task_q, result_q, semaphore)
    w.start()
    try:
        task_q.put(task)
        result_task = result_q.get()
        assert task.id == result_task.id
        errors = result_task.get_errors()
        assert task.get_errors() != errors
        print(errors)
        assert len(errors) == 1
        assert 'NotImplementedError' in errors[0]['exception']
        task_q.put(None)
        assert result_q.get() is None
    finally:
        task_q.put(None)
        w.join()


def test_worker_mp():
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=1)
    task = Task({'something': 'anything', 'parallelism': 1})
    with MockWorker(1, {}, task_q, result_q, semaphore):
        task_q.put(task)
        assert 1 == result_q.get().output['myoutput']


def test_worker_mp_parallelism_22():
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=2)
    task = Task({'something': 'anything', 'parallelism': 2})
    with MockWorker(1, {}, task_q, result_q, semaphore):
        task_q.put(task)
        assert 1 == result_q.get().output['myoutput']


def test_worker_mp_parallelism_12():
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=2)
    task = Task({'something': 'anything', 'parallelism': 1})
    with MockWorker(1, {}, task_q, result_q, semaphore):
        task_q.put(task)
        assert 1 == result_q.get().output['myoutput']


def test_execute_worker(mock_directories):
    task_q = Queue()
    result_q = Queue()
    semaphore = Semaphore(value=1)
    task = Task({
        'command': 'echo',
        'arguments': ['-n', 'hello'],
        'parallelism': 1,
    })

    worker = ExecuteWorker(1, mock_directories, task_q, result_q, semaphore)
    task_q.put(task)
    task_q.put(None)
    worker.run()
    result = result_q.get()
    data = result.get_attachment('stdout.txt')['data']
    assert 'hello' == data.decode('utf-8')
