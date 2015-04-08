#!/usr/bin/env python
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

'''
Client to run commands with.
'''
from __future__ import print_function
import simcity
from picas.iterators import (PrioritizedViewIterator, TaskViewIterator,
                             EndlessViewIterator)
import argparse
import sys
import signal


def is_cancelled():
    db = simcity.get_job_database()
    try:
        job_id = simcity.get_current_job_id()
        db.get(job_id)['cancel'] > 0
    except:
        return False


def signal_handler(signal, frame):
    print('Caught signal; finishing job.', file=sys.stderr)
    try:
        simcity.finish_job(simcity.get_job())
    except:
        pass

    sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run time")
    parser.add_argument('-D', '--days', type=int, default=1,
                        help="number of days to run the task")
    parser.add_argument('-H', '--hours', type=int, default=0,
                        help="number of hours to run the task")
    parser.add_argument('-M', '--minutes', type=int, default=0,
                        help="number of seconds to run the taskd")
    parser.add_argument('-S', '--seconds', type=int, default=0,
                        help="number of seconds to run the task")
    parser.add_argument('-p', '--padding', type=float, default=1.5,
                        help="padding factor for average task time in "
                             "calculating maximum time")
    parser.add_argument('-e', '--endless', action="store_true",
                        help="run until cancelled, even if no ")
    parser.add_argument('-P', '--prioritize', action="store_true",
                        help="prioritize tasks")
    parser.add_argument('job_id', nargs='?', help="JOB ID to assume")

    args = parser.parse_args()
    hours = args.hours + (24 * args.days)
    arg_t = args.seconds + 60 * (args.minutes + 60 * hours)

    if args.job_id is not None:
        simcity.set_current_job_id(args.job_id)

    db = simcity.get_task_database()

    if args.prioritize:
        iterator = PrioritizedViewIterator(db, 'todo_priority', 'todo')
    else:
        iterator = TaskViewIterator(db, 'todo')

    if args.endless:
        iterator = EndlessViewIterator(iterator, stop_callback=is_cancelled)

    actor = simcity.ExecuteActor(iterator=iterator)

    for sig_name in ['HUP', 'INT', 'QUIT', 'ABRT', 'TERM']:
        try:
            sig = signal.__dict__['SIG%s' % sig_name]
        except Exception as ex:
            print(ex, file=sys.stderr)
        else:
            signal.signal(sig, signal_handler)

    # Start work!
    print("Connected to the database sucessfully. Now starting work...")
    actor.run(maxtime=arg_t, avg_time_factor=args.padding)
    print("No more tasks to process, done.")
