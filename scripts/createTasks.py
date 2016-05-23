#!/usr/bin/env python
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

'''
description: create tasks with a single command
'''
from __future__ import print_function
import simcity
import sys
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="create new tasks in the database")
    parser.add_argument('command', help="command to run")
    parser.add_argument('arguments', nargs='*', help="arguments")
    parser.add_argument(
        '-n', '--number', type=int, help="number of tasks to create",
        default=1)
    parser.add_argument('-p', '--parallelism',
                        help="number of threads the task needs. Use '*' for "
                             "as many as available.", default=1)
    parser.add_argument(
        '-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()

    simcity.init(config=args.config)

    # Load the tasks to the database
    for i in range(args.number):
        try:
            simcity.add_task({
                'command': args.command,
                'arguments': args.arguments,
                'parallelism': args.parallelism,
            })
            print("added task %d" % i)
        except Exception as ex:
            print("ERROR: task {0} failed to be added: {1}".format(i, ex),
                  file=sys.stderr)
