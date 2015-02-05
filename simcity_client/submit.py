from simcity_client.util import merge_dicts
import httplib
import subprocess
from simcity_client.job import Job, queue_job

class Submitter(object):
    def __init__(self, database, host, prefix, jobdir, method):
        self.database = database
        self.host = host
        self.jobdir = jobdir
        self.prefix = prefix
        self.method = method
    
    def submit(self, command):
        job_id = self._do_submit(command)
        return queue_job(Job({'_id': self.prefix + job_id}), self.database, self.method, self.host)
        
    def _do_submit(self, command):
        raise NotImplementedError("Submission not implemented")
    
class OsmiumSubmitter(Submitter):
    __BASE = {
       "executable": "/usr/bin/qsub",
       "arguments": ["scripts/lisaSubmitExpress.sh"],
       "stderr": "stderr.txt",
       "stdout": "stdout.txt",
       "prestaged": [],
       "poststaged": []
    }
    def __init__(self, database, port, prefix, host="localhost", jobdir="~"):
        super(OsmiumSubmitter, self).__init__(database, host, prefix, jobdir, method="osmium")
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
    def __init__(self, database, host, prefix, jobdir="~"):
        super(SSHSubmitter, self).__init__(database, host, prefix, jobdir, method="ssh")
    
    def _do_submit(self, command):
        command_str = 'cd ' + self.jobdir + '; qsub ' + ' '.join(command)
        process = subprocess.Popen(['ssh', self.host, command_str], stdout=subprocess.PIPE)
        lines = process.communicate()[0].split('\n')
        try:
            # get the (before)last line
            return lines[-2]
        except:
            raise IOError("Cannot parse job ID from " + lines)
