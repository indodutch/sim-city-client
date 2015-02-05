from simcity_client.document import Token
from couchdb.http import ResourceConflict

class ViewIterator(object):
    """Dummy class to show what to implement for a PICaS iterator.
    """
    def __init__(self, database, view, **view_params):
        self._stop = False
        self.view = view
        self.view_params = view_params
        self.database = database
    
    def __repr__(self):
        return "<ViewIterator object>"
    
    def __str__(self):
        return "<view: " + self.view + ">"
    
    def __iter__(self):
        """Python needs this."""
        return self
    
    def next(self):
        if self._stop:
            raise StopIteration
        
        try:
            return self.claim_token()
        except IndexError:
            self._stop = True
            raise StopIteration
    
    def claim_token(self, allowed_failures=10):
        """
        Get the first available token from a view.
        :param allowed_failures: the number of times a lock failure may
        occur before giving up. Default=10.
        """
        raise NotImplementedError("claim_token function not implemented.")

class TokenViewIterator(ViewIterator):
    """Iterator object to fetch tokens while available.
    """
    def __init__(self, database, view, **view_params):
        """
        @param client: CouchClient for handling the connection to the CouchDB
        server.
        @param database: CouchDB view from which to fetch the token.
        @param token_modifier: instance of a TokenModifier.
        @param view_params: parameters which need to be passed on to the view
        (optional).
        """
        super(TokenViewIterator, self).__init__(database, view, **view_params)
    
    def claim_token(self, allowed_failures=10):
        for _ in xrange(allowed_failures):
            try:
                doc = self.database.get_single_from_view(self.view, window_size=100, **self.view_params)
                token = Token(doc)
                return self.database.save(token.lock())
            except ResourceConflict:
                pass

        raise EnvironmentError("Unable to claim token.")
