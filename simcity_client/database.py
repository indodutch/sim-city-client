"""
Provides access to a CouchDB database.
Originally used PiCaS, now all functionality directly accesses CouchDB.

Created on Mon Jun  4 11:40:06 2012
Updated Wed Jan 28 17:12 2015

@author: Jan Bot
@author: Joris Borgdorff
"""
from simcity_client.document import Document
import random
import numpy as np

# Couchdb imports
from couchdb.design import ViewDefinition
from couchdb.http import ResourceConflict
from couchdb.client import Server
        
class CouchDB(object):
    """Client class to handle communication with the CouchDB back-end.
    """
    def __init__(self, url="http://localhost:5984", db="test",
            username="", password="", design_document="Monitor"):
        """Create a CouchClient object. 
        :param url: the location where the CouchDB instance is located, 
        including the port at which it's listening. Default: http://localhost:5984
        :param db: the database to use. Default: test.
        # """
        self.server = Server(url=url)
        if username != "":
            self.server.resource.credentials = (username, password)
        self.db = self.server[db]
        self.design_doc = design_document
    
    def __getitem__(self, idx):
        return self.db[idx]
    
    def get_from_view(self, view, **view_params):
        """
        Get Documents from the specified view that has token _id as key.
        :param view: name of the view that has a row id coupled to a document
        :param view_params: name of the view optional extra parameters for the view.
        :return: a list of Token objects in the view
        """
        result = []
        for doc in self.view(view, **view_params):
            try:
                result.append(self.get(doc.id))
            except:
                pass # token was already deleted
            
        return result

    def get(self, id):
        """
        Get raw data associated to the given ID
        :param id: _id string of the token
        """
        data = self.db.get(id)
        if data is None:
            raise ValueError(id + " is not a document ID in the database")
        return Document(data)
    
    def get_single_from_view(self, view, window_size=1, **view_params):
        """Get a token from the specified view.
        :param view: the view to get the token from.
        :param view_params: the parameters that should be added to the view
        request. Optional.
        :return: a CouchDB token.
        """
        view = self.view(view, limit=window_size, **view_params)
        row = random.choice(view.rows)
        return self.get(row.id)
    
    def view(self, view, **view_params):
        """
        Get the data from a view
        
        :param view: name of the view
        :param view_params: the parameters that should be added to the view
        request. Optional.
        :return: iterator over the view; the rows property contains the rows.
        """
        return self.db.view(self.design_doc + '/' + view, **view_params)
    
    def save(self, doc):
        """ Save a Document to the database.
        
        Updates the document to have the new _rev value.
        :param doc: Document object
        """
        _id, _rev = self.db.save(doc.value)
        doc['_rev'] = _rev
        doc['_id'] = _id
        return doc
    
    def save_documents(self, docs):
        """Save a sequence of Documents to the database.
        
        - If the token was newly created and the _id is already is in the
          database the token will not be added.
        - If the token is an existing document, it will be updated if the _rev key
          matches.
        :param tokens [token1, token2, ...]; tokens for which the save was succesful will get new _rev values
        :return: a sequence of [succeeded1, succeeded2, ...] values.
        """
        updated = self.db.update([doc.value for doc in docs])
        
        result = np.zeros(len(docs), dtype=np.bool)
        for i in xrange(len(docs)):
            is_added, _id, _rev = updated[i]
            if is_added:
                docs[i]['_id'] = _id
                docs[i]['_rev'] = _rev
                result[i] = True
        
        return result

    def add_view(self, view, map_fun, reduce_fun=None, *args, **kwargs):
        """ Add a view to the database
        All extra parameters are passed to couchdb.design.ViewDefinition
        :param view: name of the view
        :param map_fun: string of the javascript map function
        :param reduce_fun: string of the javascript reduce function (optional)
        """
        definition = ViewDefinition(self.design_doc, view, map_fun, reduce_fun, *args, **kwargs)
        definition.sync(self.db)

    def delete(self, doc):
        """
        Delete a Document from the database

        The Document must have a valid and current _id and _rev, so they must be
        retrieved from the database and not be altered there in the mean time.
        :param doc: Document object
        :raise: ResouceConflict: if the document was updated in the database
        """
        self.db.delete(doc.value)

    def delete_documents(self, docs):
        """
        Delete a sequence of Documents from the database.
        
        The Documents must have a valid and current _id and _rev, so they must be
        retrieved from the database and not be altered there in the mean time.
        :param tokens: list of Document objects
        :return: array of booleans indicating whether the respective Document was deleted.
        """
        result = np.ones(len(docs),dtype=np.bool)
        for i, doc in enumerate(docs):
            try:
                self.delete(doc)
            except ResourceConflict as ex:
                print "Could not delete document", doc.id, " (rev", doc.rev, ") due to resource conflict:", ex
                result[i] = False
            except Exception as ex:
                print "Could not delete document ", doc, ':', ex
                result[i] = False

        return result

    def delete_from_view(self, view):
        """
        Delete all documents in a view
        
        :param view: name of the view
        :return: array of booleans indicating whether the respective tokens were deleted
        """
        docs = self.get_from_view(view)
        return self.delete_documents(docs)
