from simcity_client.util import merge_dicts
import httplib
from couchdb.http import ResourceConflict
import subprocess
from simcity_client.document import Job

class Submitter(object):
    def __init__(self, database, host, jobdir):
        self.database = database
        self.host = host
        self.jobdir = jobdir
    
    def submit(self, command):
        job_id = self._do_submit(command)
        return self.queue_job(Job({'_id': job_id}))
        
    def _do_submit(self, command):
        pass
    
    def queue_job(self, job):
        try:
            return self.database.save(job.queue(self.host))
        except ResourceConflict:
            return self.queue_job(self.database.get_job(job.id))

class OsmiumSubmitter(Submitter):
    __BASE = {
       "executable": "/usr/bin/qsub",
       "arguments": ["scripts/lisaSubmitExpress.sh"],
       "stderr": "stderr.txt",
       "stdout": "stdout.txt",
       "prestaged": [],
       "poststaged": []
    }
    def __init__(self, database, port, host="localhost", jobdir="~"):
        super(OsmiumSubmitter, self).__init__(database, host, jobdir)
        self.port = port
        
    def _do_submit(self, command):
        request = merge_dicts(OsmiumSubmitter.__BASE, {'arguments': command, 'jobdir': self.jobdir})
        conn = httplib.HTTPConnection(self.host, self.port)
        conn.request("POST", request)
        response = conn.getresponse()
        conn.close()
        
        if response.status != 201:
            raise IOError("Cannot submit job: " + response.reason + "(HTTP status " + response.status + ")")
                
        return response.location.split('/')[-1]

class SSHSubmitter(Submitter):
    def __init__(self, database, host, jobdir="~"):
        super(SSHSubmitter, self).__init__(database, host, jobdir)
    
    def _do_submit(self, command):
        command_str = 'cd ' + self.jobdir + '; qsub ' + ' '.join(command)
        process = subprocess.Popen(['ssh', self.host, command_str], stdout=subprocess.PIPE)
        lines = process.communicate()[0].split('\n')
        try:
            # get the first item of the last line
            return lines[-2].split('.')[0]
        except:
            raise IOError("Cannot parse job ID. Last line: " + last)
