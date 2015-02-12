from simcity.document import Document
from simcity.util import seconds

class Job(Document):
    __BASE = {
        'type': 'job',
        'hostname': '',
        'start': 0,
        'done': 0,
        'queue': 0,
        'method': '',
        'archive': 0
    }
    
    def __init__(self, job):        
        super(Job, self).__init__(job, Job.__BASE)
        if '_id' not in self.doc:
            raise ValueError('Job ID must be set')
    
    def queue(self, method, host = None):
        self.doc['method'] = method
        if host is not None:
            self.doc['hostname'] = host
        self.doc['queue'] = seconds()
        return self
    
    def start(self):
        self._update_hostname()
        self.doc['start'] = seconds()
        return self

    def finish(self):
        self.doc['done'] = seconds()
        return self
    
    def archive(self):
        self.doc['archive'] = seconds()
        self.id = 'archived-' + self.id + '-' + str(seconds())
        del self.doc['_rev']
        return self
