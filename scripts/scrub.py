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
Remove lock and done from tasks and remove queued and active from jobs.
'''
from __future__ import print_function
import simcity
import argparse
import time


def update_task(id):
    return simcity.get_task(id).scrub()


def update_job(id):
    return simcity.get_job(id).archive()


if __name__ == '__main__':
    task_views = ['locked', 'error']
    job_views = ['pending_jobs', 'active_jobs', 'finished_jobs']
    parser = argparse.ArgumentParser(description="Make old locked tasks"
                                     "available for processing again "
                                     "(default: all)")
    parser.add_argument('-D', '--days', type=int, default=0,
                        help="number of days ago the task was locked")
    parser.add_argument('-H', '--hours', type=int, default=0,
                        help="number of hours ago the task was locked")
    parser.add_argument('-M', '--minutes', type=int, default=0,
                        help="number of minutes ago the task was locked")
    parser.add_argument('-S', '--seconds', type=int, default=0,
                        help="number of seconds ago the task was locked")
    parser.add_argument('-c', '--config', default=None,
                        help="configuration file")
    parser.add_argument('view', choices=task_views + job_views,
                        default='locked', help="view to scrub")
    args = parser.parse_args()

    hours = args.hours + (24 * args.days)
    arg_t = args.seconds + 60 * (args.minutes + 60 * hours)
    min_t = int(time.time()) - arg_t

    simcity.init(configfile=args.config)

    if args.view in task_views:
        db = simcity.get_task_database()
        do_update = update_task
    else:
        db = simcity.get_job_database()
        do_update = update_job

    total = 0
    updates = []
    for row in db.view(args.view):
        total += 1
        if arg_t <= 0 or row.value['lock'] < min_t:
            doc = do_update(row.id)
            updates.append(doc)

    if len(updates) > 0:
        db.save_documents(updates)
        print("Scrubbed %d out of %d documents from '%s'" %
              (len(updates), total, args.view))
    else:
        print("No scrubbing required")
