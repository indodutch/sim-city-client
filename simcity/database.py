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

""" CouchDB helper class """

from __future__ import print_function
from .document import Document
import random
import sys
import couchdb
from couchdb.design import ViewDefinition
from couchdb.http import ResourceConflict


class CouchDB(object):

    """Client class to handle communication with the CouchDB back-end.
    """

    def __init__(self, url="http://localhost:5984", db="test",
                 username=None, password="", ssl_verification=True,
                 create=False):
        """Create a CouchClient object.
        :param url: the location where the CouchDB instance is located,
                    including the port at which it's listening.
                    Default: http://localhost:5984
        :param db: the database to use. Default: test.
        """
        server = couchdb.Server(url)
        if username is not None:
            server.resource.credentials = (username, password)
        if not ssl_verification:
            server.resource.session.disable_ssl_verification()

        if create:
            self.db = server.create(db)
        else:
            self.db = server[db]

    def copy(self):
        """ Copy the database in a Thread and Process-safe way."""
        resource = self.db.resource
        try:
            username, password = resource.credentials
        except TypeError:
            username, password = None, ""

        return CouchDB(
            url=resource.url[:-len(self.db.name) - 1], db=self.db.name,
            username=username, password=password,
            ssl_verification=resource.session._disable_ssl_verification)

    def __getitem__(self, idx):
        return self.db[idx]

    def get_from_view(self, view, **view_params):
        """
        Get Documents from the specified view that has task _id as key.
        :param view: name of the view that has a row id coupled to a document
        :param view_params: name of the view optional extra parameters for the
                            view.
        :return: a list of Task objects in the view
        """
        result = []
        for doc in self.view(view, **view_params):
            try:
                result.append(self.get(doc.id))
            except:
                pass  # doc was already deleted

        return result

    def get(self, id):
        """
        Get raw data associated to the given ID
        :param id: _id string of the task
        """
        data = self.db.get(id)
        if data is None:
            raise ValueError(id + " is not a document ID in the database")
        return Document(data)

    def get_single_from_view(self, view, window_size=1, **view_params):
        """Get a document from the specified view.
        :param view: the view to get the document from.
        :param view_params: the parameters that should be added to the view
        request. Optional.
        :param window_size: the size of the initial request to CouchDB, only
        one record within that set, which is randomly selected, is returned.
        :return: a CouchDB document.
        """
        view = self.view(view, limit=window_size, **view_params)
        row = random.choice(view.rows)
        return self.get(row.id)

    def view(self, view, design_doc="Monitor", **view_params):
        """
        Get the data from a view

        :param view: name of the view
        :param view_params: the parameters that should be added to the view
        request. Optional.
        :return: iterator over the view; the rows property contains the rows.
        """
        return self.db.view(design_doc + '/' + view, **view_params)

    def save(self, doc):
        """ Save a Document to the database.

        Updates the document to have the new _rev value.
        :param doc: Document object
        :throws couchdb.http.ResourceConflict: when document exists with
                different revision or was deleted.
        """
        _id, _rev = self.db.save(doc.value)
        doc['_rev'] = _rev
        doc['_id'] = _id
        return doc

    def save_documents(self, docs):
        """Save a sequence of Documents to the database.

        - If the document was newly created and the _id is already is in the
          database the document will not be added.
        - If the document is an existing document, it will be updated if the
          _rev key matches.

        :param tasks [task1, task2, ...]; tasks for which the save was
                succesful will get new _rev values
        :return: a sequence of [succeeded1, succeeded2, ...] values.
        """
        updated = self.db.update([doc.value for doc in docs])

        result = [False] * len(docs)
        for i in range(len(docs)):
            is_added, _id, _rev = updated[i]
            if is_added:
                docs[i]['_id'] = _id
                docs[i]['_rev'] = _rev
                result[i] = True

        return result

    def add_view(self, view, map_fun, reduce_fun=None, design_doc="Monitor",
                 *args, **kwargs):
        """ Add a view to the database
        All extra parameters are passed to couchdb.design.ViewDefinition
        :param view: name of the view
        :param map_fun: string of the javascript map function
        :param reduce_fun: string of the javascript reduce function (optional)
        """
        definition = ViewDefinition(
            design_doc, view, map_fun, reduce_fun, *args, **kwargs)
        definition.sync(self.db)

    def delete(self, doc):
        """
        Delete a Document from the database

        The Document must have a valid and current _id and _rev, so they must
        be retrieved from the database and not be altered there in the mean
        time.
        :param doc: Document object
        :raise: ResouceConflict: if the document was updated in the database
        """
        self.db.delete(doc.value)

    def delete_documents(self, docs):
        """
        Delete a sequence of Documents from the database.

        The Documents must have a valid and current _id and _rev, so they must
        be retrieved from the database and not be altered there in the mean
        time.
        :param tasks: list of Document objects
        :return: array of booleans indicating whether the respective Document
                was deleted.
        """
        result = [True] * len(docs)
        for i, doc in enumerate(docs):
            try:
                self.delete(doc)
            except ResourceConflict as ex:
                print("Could not delete document %s (rev %s) "
                      "due to resource conflict: %s"
                      % (doc.id, doc.rev, str(ex)), file=sys.stderr)
                result[i] = False
            except Exception as ex:
                print("Could not delete document %s: %s"
                      % (str(doc), str(ex)), file=sys.stderr)
                result[i] = False

        return result

    def delete_from_view(self, view, design_doc="Monitor"):
        """
        Delete all documents in a view

        :param view: name of the view
        :return: array of booleans indicating whether the respective tasks
                were deleted
        """
        docs = self.get_from_view(view, design_doc=design_doc)
        return self.delete_documents(docs)

    def set_users(self, admins=None, members=None, admin_roles=None,
                  member_roles=None):
        """ Set the members and admins of the database. """
        security = self.db.resource.get_json("_security")[2]

        def try_set(value, d, key, sub_key):
            """ In a double dictionary, set value in d[key][sub_key] if value
            is not None """
            if value is not None:
                try:
                    d[key][sub_key] = value
                except KeyError:
                    d[key] = {sub_key: value}

        try_set(admins, security, 'admins', 'names')
        try_set(members, security, 'members', 'names')
        try_set(admin_roles, security, 'admins', 'roles')
        try_set(member_roles, security, 'members', 'roles')

        self.db.resource.put("_security", security)
