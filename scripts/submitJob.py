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
Combines createTask and startJob, to create a task from a command and then start a job. 
'''
import simcity
import argparse
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start a job")
    parser.add_argument('command', help="command to run")
    parser.add_argument('host', help="host to run pilot job on")
    parser.add_argument('-m', '--max', help="only run if there are less than MAX jobs running", default=2)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args() 

    simcity.init(configfile=args.config)
    try:
        task = simcity.task.add({'command': args.command})
        print "task", task.id, "added to the database"
    except Exception as ex:
        print "Task could not be added to the database:", ex
        sys.exit(1)

    job = simcity.job.submit_if_needed(args.host, args.max)
    if job is None:
        print "Let task be processed by existing pilot-job scripts"
    else:
        print "Job " + job['batch_id'] + " (ID: " + job.id + ") will process task"
