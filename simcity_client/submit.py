from simcity_client.util import merge_dicts
import httplib
import subprocess
from simcity_client.job import Job, queue_job, archive_job
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
        job = queue_job(Job({'_id': job_id}), self.database, self.method, self.host)
        try:
            job['batch_id'] = self._do_submit(job, command)
        except Exception as ex:
            archive_job(job, self.database)
            raise ex
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
        process = subprocess.Popen(['ssh', self.host, command_str], stdout=subprocess.PIPE)
        lines = process.communicate()[0].split('\n')
        try:
            # get the (before)last line
            return lines[-2]
        except:
            raise IOError("Cannot parse job ID from " + lines)
