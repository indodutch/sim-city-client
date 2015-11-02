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

from __future__ import print_function

import simcity
from .util import listfiles, write_json, expandfilename
from .task import upload_attachment
from picas import RunActor

import os
from subprocess import call


class ExecuteActor(RunActor):
    """
    Executes a job locally, all tasks provided by its iterator.

    Tasks are assumed to have input parameters. It creates a new input, output
    and temporary directory for each tasks, and attaches files that are
    generated in the output directory to the task when the task is finished.

    If the command exits with non-zero, the task is assumed to have failed.

    At the start of a job, it is registered to the job database, when it is
    finished, it is archived.
    """
    def __init__(self, task_db=None, iterator=None, job_db=None, config=None):
        if task_db is None:
            task_db = simcity.get_task_database()
        super(ExecuteActor, self).__init__(task_db, iterator=iterator)

        if job_db is None:
            job_db = simcity.get_job_database()
        self.job_db = job_db

        if config is None:
            config = simcity.get_config()
        self.config = config.section('Execution')

    def prepare_env(self, *kargs, **kwargs):
        self.job = simcity.start_job(database=self.job_db)

    def cleanup_env(self, *kargs, **kwargs):
        self.job['tasks_processed'] = self.tasks_processed
        simcity.finish_job(self.job, database=self.job_db)

    def process_task(self, task):
        print("-----------------------")
        print("Working on task: {0}".format(task.id))

        dirs = self.create_dirs(task)
        params_file = os.path.join(dirs['input'], 'input.json')
        write_json(params_file, task.input)

        task['execute_properties'] = {'dirs': dirs, 'input_file': params_file}

        command = [expandfilename(task['command']), dirs['tmp'],
                   dirs['input'], dirs['output']]
        stdout = os.path.join(dirs['output'], 'stdout')
        stderr = os.path.join(dirs['output'], 'stderr')
        try:
            if self.execute(command, stdout, stderr) != 0:
                task.error("Command failed")
        except Exception as ex:
            task.error("Command raised exception", ex)

        task.output = {}

        # Read all files in as attachments
        out_files = listfiles(dirs['output'])
        for filename in out_files:
            upload_attachment(task, dirs['output'], filename)

        if not task.has_error():  # don't override error status
            task.done()
        print("-----------------------")

    def execute(self, command, stdoutFile, stderrFile):
        with open(stdoutFile, 'w') as fout:
            with open(stderrFile, 'w') as ferr:
                return call(command, stdout=fout, stderr=ferr)

    def create_dirs(self, task):
        dir_map = {
            'tmp':    'tmp_dir',
            'input':  'input_dir',
            'output': 'output_dir'
        }

        dirs = {}
        for d, conf in dir_map.items():
            superdir = expandfilename(self.config[conf])
            try:
                os.mkdir(superdir)
            except OSError:  # directory exists
                pass

            dirs[d] = os.path.join(superdir,
                                   task.id + '_' + str(task['lock']))
            os.mkdir(dirs[d])

        return dirs
