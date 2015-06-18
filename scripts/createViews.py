#!/usr/bin/env python
# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>,
#                Anatoli Danezi  <anatoli.danezi@surfsara.nl>
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
Create views necessary to run simcity client with.
'''
import pystache
import simcity
import argparse


def createViews():
    taskMapTemplate = '''
function(doc) {
  if(doc.type === 'task' && {{condition}}) {
    emit(doc._id, {
        lock: doc.lock,
        done: doc.done,
    });
  }
}
    '''
    erroneousMapCode = '''
function(doc) {
  if (doc.type === 'task' && doc.lock == -1) {
    emit(doc._id, doc.error);
  }
}
    '''
    jobMapTemplate = '''
function(doc) {
  if (doc.type == 'job' && {{condition}}) {
    emit(doc._id, {
        queue: doc.queue,
        start: doc.start,
        done: doc.done,
    });
  }
}
    '''
    overviewMapTemplate = '''
function(doc) {
  if(doc.type === 'task') {
  {{#tasks}}
    if ({{condition}}) {
      emit("{{name}}", 1);
    }
  {{/tasks}}
    if (doc.lock === -1) {
      emit("error", 1);
    }
  }
  if (doc.type === 'job') {
  {{#jobs}}
    if ({{condition}}) {
      emit('{{name}}', 1);
    }
  {{/jobs}}
  }
}
    '''
    overviewReduceCode = '''
function (key, values, rereduce) {
   return sum(values);
}
    '''

    tasks = {
        'todo':     'doc.lock === 0',
        'todo_priority': 'doc.lock === 0 && doc.priority === "high"',
        'locked':   'doc.lock > 0 && doc.done === 0',
        'done':     'doc.lock > 0 && doc.done > 0'
    }
    jobs = {
        'pending_jobs':  'doc.start === 0 && doc.archive === 0',
        'active_jobs':  'doc.start > 0 && doc.done == 0 && doc.archive === 0',
        'finished_jobs': 'doc.done > 0',
        'archived_jobs': 'doc.archive > 0'
    }
    pystache_views = {
        'tasks': [{'name': view, 'condition': condition}
                  for view, condition in tasks.items()],
        'jobs':  [{'name': view, 'condition': condition}
                  for view, condition in jobs.items()]
    }
    renderer = pystache.renderer.Renderer(escape=lambda u: u)

    task_db = simcity.get_task_database()
    job_db = simcity.get_job_database()

    for view in pystache_views['tasks']:
        mapCode = renderer.render(taskMapTemplate, view)
        task_db.add_view(view['name'], mapCode)

    for view in pystache_views['jobs']:
        mapCode = renderer.render(jobMapTemplate, view)
        job_db.add_view(view['name'], mapCode)

    task_db.add_view('error', erroneousMapCode)

    # overview_total View -- lists all views and the number of tasks in each
    # view
    overviewMapCode = renderer.render(overviewMapTemplate, pystache_views)
    task_db.add_view('overview_total', overviewMapCode, overviewReduceCode)
    job_db.add_view(
        'overview_total', overviewMapCode, overviewReduceCode)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="create views in the database")
    parser.add_argument(
        '-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()

    simcity.init(config=args.config)
    # Create the Views in database
    createViews()
