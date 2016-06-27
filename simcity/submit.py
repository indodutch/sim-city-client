# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center
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

""" Submit jobs to physical infrastructure. """

from .util import merge_dicts
from .document import Job
import simcity
import json
import xenon

try:
    from httplib import HTTPConnection
except ImportError:
    from http.client import HTTPConnection

import subprocess
from uuid import uuid4


def submit_if_needed(hostname, max_jobs, submitter=None):
    """
    Submit a new job if not enough jobs are already running or queued.

    Host configuration is extracted from an entry in the global config file.
    """

    num = simcity.overview_total()

    num_jobs = num['active_jobs'] + num['pending_jobs']
    num_tasks = num['todo'] + num['locked']
    if num_jobs < min(num_tasks, max_jobs):
        return submit(hostname, submitter)
    else:
        return None


def submit(hostname, submitter=None):
    """ Submit a new job to given host. """
    host = hostname + '-host'
    try:
        host_cfg = simcity.get_config().section(host)
    except KeyError:
        raise ValueError('%s not configured under %s section' %
                         (hostname, host))

    try:
        if submitter is None:
            if host_cfg['method'] == 'ssh':
                submitter = SSHSubmitter(
                    database=simcity.get_job_database(),
                    host=host_cfg['host'],
                    jobdir=host_cfg['path'],
                    prefix=hostname + '-')
            elif host_cfg['method'] == 'osmium':
                try:
                    osmium_cfg = simcity.get_config().section('osmium')
                except KeyError:
                    osmium_cfg = {'host': 'localhost:9998'}

                submitter = OsmiumSubmitter(
                    database=simcity.get_job_database(),
                    launcher=host_cfg.get('launcher'),
                    url=host_cfg.get('host', osmium_cfg['host']),
                    jobdir=host_cfg['path'],
                    prefix=hostname + '-',
                    max_time=host_cfg.get('max_time'))
            elif host_cfg['method'] == 'xenon':
                submitter = XenonSubmitter(
                    database=simcity.get_job_database(),
                    host=host_cfg['host'],
                    jobdir=host_cfg['path'],
                    prefix=hostname + '-',
                    max_time=host_cfg.get('max_time', 1440))
            else:
                raise EnvironmentError('Connection method for %s unknown' %
                                       hostname)

        script = [host_cfg['script']] + host_cfg.get('arguments', '').split()
    except KeyError:
        raise EnvironmentError(
            "Connection method for %s not well configured" % hostname)

    return submitter.submit(script)


class Submitter(object):

    """ Submits a job """

    def __init__(self, database, host, prefix, jobdir, method):
        self.database = database
        self.host = host
        self.jobdir = jobdir
        self.prefix = prefix
        self.method = method

    def submit(self, command):
        """
        Submit given command to the configured host.

        Raises IOError if the command could not be submitted.
        """
        job_id = 'job_' + self.prefix + uuid4().hex
        job = simcity.queue_job(Job({'_id': job_id}), self.method,
                                host=self.host, database=self.database)
        try:
            job['batch_id'] = self._do_submit(job, command)
        except:
            simcity.archive_job(job, database=self.database)
            raise
        else:
            self.database.save(job)
            return job

    def _do_submit(self, job, command):
        """
        Submit given command, using job metadata.

        Override in subclasses.
        """
        raise NotImplementedError


class OsmiumSubmitter(Submitter):

    """ Submits a job to Osmium. """
    __BASE = {
        "prestaged": [],
        "poststaged": [],
        "environment": {}
    }

    def __init__(self, database, url, prefix, launcher, jobdir="~",
                 max_time=None):
        super(OsmiumSubmitter, self).__init__(
            database, url, prefix, jobdir, method="osmium")
        self.launcher = launcher
        self.max_time = max_time

    def _request(self, location="/", method="GET", data=None):
        """ Make a HTTPS request to the Osmium service. """
        conn = HTTPConnection(self.host)
        url = 'http://{0}{1}'.format(self.host, location)
        conn.request(method, url, data)
        response = conn.getresponse()
        conn.close()
        if response.status < 200 or response.status >= 300:
            raise IOError("Request failed " + response.reason +
                          "(HTTP status " + response.status + ")")
        return response

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        request = merge_dicts(OsmiumSubmitter.__BASE, {
            'executable':  command[0],
            'arguments':   command[1:],
            'jobdir':      self.jobdir,
            'environment': {'SIMCITY_JOBID': job.id},
        })
        if self.launcher is not None:
            request['launcher'] = self.launcher
        if self.max_time is not None:
            request['max_time'] = int(self.max_time)

        response = self._request(method="POST", data=request)
        return response.location.split('/')[-1]

    def status(self, osmium_job_id):
        """ Get the status dict of a single job from Osmium. """
        response = self._request('/job/{0}'.format(osmium_job_id))
        return json.loads(response.data)

    def jobs(self):
        """ Get a list of jobs that are currently running. """
        response = self._request('/job')
        return json.loads(response.data)


class SSHSubmitter(Submitter):

    """ Submits a job over SSH, remotely running the Torque qsub utility. """

    def __init__(self, database, host, prefix, jobdir="~"):
        super(SSHSubmitter, self).__init__(
            database, host, prefix, jobdir, method="ssh")

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        command_str = ('cd "%s";'
                       'export SIMCITY_JOBID="%s";'
                       'qsub -v SIMCITY_JOBID %s') % (self.jobdir, job.id,
                                                      ' '.join(command))
        process = subprocess.Popen(['ssh', self.host, command_str],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (stdout, stderr) = process.communicate()
        lines = stdout.decode('utf-8').split('\n')
        try:
            # get the (before)last line
            return lines[-2]
        except IndexError:
            raise IOError("Cannot parse job ID from stdout: '%s'\n"
                          "==== stderr ====\n'%s'"
                          % (stdout, stderr))


class XenonSubmitter(Submitter):
    xenon_init = False

    """ Submits job using Xenon. """
    def __init__(self, database, host, prefix, jobdir, max_time=1440):
        super(XenonSubmitter, self).__init__(database, host, prefix, jobdir,
                                             "xenon")
        XenonSubmitter.init()
        self.max_time = max_time

    @classmethod
    def init(cls, log_level='INFO'):
        """ Initialize Xenon. The method is a no-op after the first call. """
        if not cls.xenon_init:
            cls.xenon_init = True
            xenon.init(log_level=log_level)

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        with xenon.Xenon() as x:
            try:
                jobs = x.jobs()
                desc = xenon.jobs.JobDescription()
                desc.addEnvironment('SIMCITY_JOBID', job.id)
                desc.setWorkingDirectory(self.jobdir)
                desc.setExecutable(command[0])
                desc.setArguments(command[1:])
                urlsplit = self.host.split('://')
                if len(urlsplit) != 2:
                    raise ValueError("host must contain a scheme and a "
                                     "hostname, syntax `scheme://host`.")
                scheme, hostname = urlsplit
                scheduler = jobs.newScheduler(scheme, hostname, None, None)
                job = jobs.submitJob(scheduler, desc)
                if scheduler.isOnline():
                    print("Waiting for submission to finish...")
                    jobs.waitUntilDone(job, 0)
                    print("Done.")

                return job.getIdentifier()
            except xenon.exceptions.XenonException as ex:
                raise IOError(ex.javaClass(),
                              "Cannot submit job with Xenon: {0}"
                              .format(ex.message()))
