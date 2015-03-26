# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>
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
# limitations under the License.

from picas.util import merge_dicts
from picas.documents import Job
import simcity

from numbers import Number
import httplib
import subprocess
from uuid import uuid4


def submit_if_needed(hostname, max_jobs, submitter=None):
    if not isinstance(max_jobs, Number):
        raise ValueError("Max jobs must be a number")

    num = simcity.overview_total()

    num_jobs = num['active_jobs'] + num['pending_jobs']
    if num_jobs < num['todo'] and num_jobs < max_jobs:
        return submit(hostname)
    else:
        return None


def submit(hostname, submitter=None):
    simcity.check_init()

    host = hostname + '-host'
    try:
        host_cfg = simcity.config.section(host)
    except:
        raise ValueError(
            hostname + ' not configured under ' + host + 'section')

    try:
        if submitter is None:
            if host_cfg['method'] == 'ssh':
                submitter = SSHSubmitter(
                    database=simcity.job_database,
                    host=host_cfg['host'],
                    jobdir=host_cfg['path'],
                    prefix=hostname + '-')
            elif host_cfg['method'] == 'osmium':
                submitter = OsmiumSubmitter(
                    database=simcity.job_database,
                    port=host_cfg['port'],
                    jobdir=host_cfg['path'],
                    prefix=hostname + '-')
            else:
                raise EnvironmentError(
                    'Connection method for ' + hostname + ' unknown')

        script = [host_cfg['script']]
    except KeyError:
        raise EnvironmentError(
            "Connection method for %s not well configured" % hostname)

    return submitter.submit(script)


class Submitter(object):

    def __init__(self, database, host, prefix, jobdir, method):
        self.database = database
        self.host = host
        self.jobdir = jobdir
        self.prefix = prefix
        self.method = method

    def submit(self, command):
        job_id = 'job_' + self.prefix + uuid4().hex
        job = simcity.queue_job(Job({'_id': job_id}), self.method, self.host)
        try:
            job['batch_id'] = self._do_submit(job, command)
        except:
            simcity.archive_job(job)
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
        super(OsmiumSubmitter, self).__init__(
            database, host, prefix, jobdir, method="osmium")
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
            raise IOError("Cannot submit job: " + response.reason +
                          "(HTTP status " + response.status + ")")

        return response.location.split('/')[-1]


class SSHSubmitter(Submitter):

    def __init__(self, database, host, prefix, jobdir="~"):
        super(SSHSubmitter, self).__init__(
            database, host, prefix, jobdir, method="ssh")

    def _do_submit(self, job, command):
        command_str = ('cd "%s";'
                       'export SIMCITY_JOBID="%s";'
                       'qsub -v SIMCITY_JOBID %s') % (self.jobdir, job.id,
                                                      ' '.join(command))
        process = subprocess.Popen(['ssh', self.host, command_str],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (stdout, stderr) = process.communicate()
        lines = stdout.split('\n')
        try:
            # get the (before)last line
            return lines[-2]
        except:
            raise IOError("Cannot parse job ID from stdout: '" +
                          stdout + "'\n==== stderr ====\n'" + stderr + "'")
