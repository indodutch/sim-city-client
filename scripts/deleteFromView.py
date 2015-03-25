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

''' Delete all documents from given view '''

import simcity
import argparse

if __name__ == '__main__':
    task_views = ['todo', 'done', 'locked']
    job_views = [
        'pending_jobs', 'active_jobs', 'finished_jobs', 'archived_jobs']
    parser = argparse.ArgumentParser(description="Remove all tasks in a view")
    parser.add_argument('view', choices=task_views + job_views,
                        help="View to remove documents from")
    parser.add_argument('-c', '--config', help="configuration file",
                        default=None)
    args = parser.parse_args()

    simcity.init(configfile=args.config)

    if args.view in task_views:
        db = simcity.task.database
    else:
        db = simcity.job.database

    is_deleted = db.delete_from_view(args.view)
    print("Deleted", sum(is_deleted), "out of", len(is_deleted),
          "tasks from view", args.view)
