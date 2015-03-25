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
# python imports
import simcity
from simcity.job import ExecuteActor
import argparse

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
    parser.add_argument('job_id', nargs='?', help="JOB ID to assume")

    args = parser.parse_args()
    arg_t = args.seconds + 60 * \
        (args.minutes + 60 * (args.hours + (24 * args.days)))

    if args.job_id is not None:
        simcity.job.job_id = args.job_id

    actor = ExecuteActor()

    # Start work!
    print "Connected to the database sucessfully. Now starting work..."
    actor.run(maxtime=arg_t, avg_time_factor=args.padding)
    print "No more tasks to process, done."
