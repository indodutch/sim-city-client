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

import simcity
from simcity import database
import simcity.task
import simcity.job

config = None
is_initialized = False
__is_initializing = True


def _check_init():
    if not is_initialized:
        raise EnvironmentError(
            "Databases are not initialized yet, please provide a valid "
            "configuration file to simcity.init()")


def init(configfile):
    global is_initialized, config

    try:
        config = simcity.util.Config(configfile)
    except:
        # default initialization may fail
        if not __is_initializing:
            raise
    else:
        try:
            simcity.task.database = database._load('task-db')
        except:
            if not __is_initializing:
                raise

        try:
            simcity.job.database = database._load('job-db')
        except EnvironmentError:
            # job database not explicitly configured
            simcity.job.database = simcity.task.database
        except:
            if not __is_initializing:
                raise

        is_initialized = True


def overview_total():
    _check_init()

    views = ['todo', 'locked', 'error', 'done',
             'finished_jobs', 'active_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in simcity.task.database.view('overview_total', group=True):
        num[view.key] = view.value

    if simcity.job.database is not simcity.task.database:
        for view in simcity.job.database.view('overview_total', group=True):
            num[view.key] = view.value

    return num


init(None)
__is_initializing = False
