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
Start a new job on the infrastructure to process the tasks.
'''
import simcity
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start a job")
    parser.add_argument('host', help="host to run pilot job on")
    parser.add_argument('-m', '--max', help="only run if there are less than MAX jobs running", default=2)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()

    simcity.init(configfile=args.config)
    
    job = simcity.job.submit_if_needed(args.host, args.max)
    if job is None:
        print "No tasks to process or already " + str(args.max) + " jobs running (increase maximum number of jobs with -m)"
    else:
        print "Job " + job['batch_id'] + " (ID: " + job.id + ") started"
