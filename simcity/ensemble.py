# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center, Jan Bot
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
# limitations under the License."""

"""
Ensemble metadata
"""


def ensemble_view(task_db, name, version, url=None, ensemble=None):
    """
    Create a view for an ensemble.

    This checks if the view already exists. If not, a new view is created with
    a new design document name. The view is then called all_docs under that
    design document.

    @param task_db: task database
    @param name: simulator name
    @param version: simulator version
    @param url: base url of the database. If none, use the task database url.
    @param ensemble: ensemble name. If None, no selection on ensemble is made.
    @return design document
    """
    if ensemble is None:
        design_doc = '{0}_{1}'.format(name, version)
        ensemble_condition = ''
    else:
        design_doc = '{0}_{1}_{2}'.format(name, version, ensemble)
        ensemble_condition = ' && doc.ensemble === "{0}"'.format(ensemble)

    doc_id = '_design/{0}'.format(design_doc)
    try:
        task_db.get(doc_id)
    except ValueError:
        if url is None:
            url = task_db.url

        if not url.endswith('/'):
            url += '/'

        map_fun = '''
            function(doc) {{
              if (doc.type === "task" && doc.name === "{name}" &&
                  doc.version === "{version}" && !doc.archive
                  {ensemble_condition}) {{
                emit(doc._id, {{
                  _id: doc._id,
                  _rev: doc._rev,
                  url: "{url}" + doc._id,
                  input: {{
                    ensemble: doc.input.ensemble,
                    simulation: doc.input.simulation,
                  }},
                  error: doc.error,
                  lock: doc.lock,
                  done: doc.done,
                  input: doc.input
                }});
              }}
            }}'''.format(name=name, version=version,
                         ensemble_condition=ensemble_condition, url=url)

        task_db.add_view('all_docs', map_fun, design_doc=design_doc)

    return design_doc
