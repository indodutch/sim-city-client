from simcity_client.execute import RunActor
from simcity_client.document import Document
import random

class MockDB(object):
    TOKENS = [{'_id': 'a', 'lock': 0}, {'_id': 'b', 'lock': 0}]
    JOBS = [{'_id': 'myjob'}]
    def __init__(self):
        global tokens
        self.tokens = {t['_id']: t.copy() for t in MockDB.TOKENS} # deep copy
        self.jobs = {t['_id']: t.copy() for t in MockDB.JOBS}
        self.saved = {}

    def get_single_from_view(self, view, **view_params):
        idx = random.choice(self.tokens.keys())
        return Document(self.tokens[idx])
    
    def get(self, idx):
        if idx in self.saved:
            return self.saved[idx]
        elif idx in self.tokens:
            return self.tokens[idx]
        elif idx in self.jobs:
            return self.jobs[idx]
        else:
            raise KeyError
    
    def save(self, doc):
        doc['_rev'] = 'something'

        if doc.id in self.jobs:
            self.jobs[doc.id] = doc.value
        else:
            if doc.id in self.tokens:
                del self.tokens[doc.id]        
            self.saved[doc.id] = doc.value
        
        return doc

class MockRun(RunActor):
    JOB_ID = 'myjob'
    def __init__(self, callback):
        super(MockRun, self).__init__(MockDB(), MockDB.JOBS[0]['_id'])
        self.callback = callback
    
    def process_token(self, token):
        self.callback(token)