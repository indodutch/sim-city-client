from simcity.util import merge_dicts
import simcity.job
from simcity.job.document import Job

import httplib
import subprocess
from uuid import uuid4

class Submitter(object):
    def __init__(self, database, host, prefix, jobdir, method):
        self.database = database
        self.host = host
        self.jobdir = jobdir
        self.prefix = prefix
        self.method = method
    
    def submit(self, command):
        job_id = 'job_' + self.prefix + uuid4().hex
        job = simcity.job.queue(Job({'_id': job_id}), self.method, self.host)
        try:
            job['batch_id'] = self._do_submit(job, command)
        except:
            simcity.job.archive(job)
            raise
        else:
            self.database.save(job)
            return job
                    
    def _do_submit(self, job, command):
        raise NotImplementedError
    
class OsmiumSubmitter(Submitter):
    __BASE = {
       "executable": "/usr/bin/qsub",
       "arguments": ["lisaSubmitExpress.sh"],
       "stderr": "stderr.txt",
       "stdout": "stdout.txt",
       "prestaged": [],
       "poststaged": [],
       "environment": {}
    }
    def __init__(self, database, port, prefix, host="localhost", jobdir="~"):
        super(OsmiumSubmitter, self).__init__(database, host, prefix, jobdir, method="osmium")
        self.port = port
        
    def _do_submit(self, job, command):
        request = merge_dicts(OsmiumSubmitter.__BASE, {
            'arguments':   command,
            'jobdir':      self.jobdir,
            'environment': {'SIMCITY_JOBID': job.id}
        })

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
    
    def _do_submit(self, job, command):
        commandTemplate = 'cd "%s"; export SIMCITY_JOBID="%s"; qsub -v SIMCITY_JOBID %s'
        command_str = commandTemplate % (self.jobdir, job.id, ' '.join(command))
        process = subprocess.Popen(['ssh', self.host, command_str], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = process.communicate()
        lines = stdout.split('\n')
        try:
            # get the (before)last line
            return lines[-2]
        except:
            raise IOError("Cannot parse job ID from stdout: '" + stdout + "'\n==== stderr ====\n'" + stderr + "'")
