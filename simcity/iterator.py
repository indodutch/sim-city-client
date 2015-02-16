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
            return self.claim_task()
        except IndexError:
            self._stop = True
            raise StopIteration
    
    def claim_task(self, allowed_failures=10):
        """
        Get the first available task from a view.
        :param allowed_failures: the number of times a lock failure may
        occur before giving up. Default=10.
        """
        raise NotImplementedError("claim_task function not implemented.")
