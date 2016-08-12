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

from .document import Job
from .job import archive_job, queue_job
from .management import get_config, get_job_database
import json
try:
    from httplib import HTTPConnection
except ImportError:
    from http.client import HTTPConnection
import subprocess
from uuid import uuid4

try:
    import xenon
    import jpype
    xenon_support = True
except ImportError:
    xenon_support = False


def create_adaptor(host_id, host_cfg):
    """ Create the appropriate adaptor given the host_id and host config.

    @param host_id: host_id as mentioned in the configuration
    @param host_cfg: configuration dict of the host
    @raise EnvironmentError: if host_cfg does not configure all needed
        properties
    @return: Adaptor
    """
    global xenon_support
    try:
        if host_cfg['method'] == 'ssh':
            return SSHAdaptor(
                database=get_job_database(),
                host=host_cfg['host'],
                jobdir=host_cfg['path'],
                prefix=host_id + '-')
        elif host_cfg['method'] == 'osmium':
            try:
                osmium_cfg = get_config().section('osmium')
            except KeyError:
                osmium_cfg = {'host': 'localhost:9998'}

            return OsmiumAdaptor(
                database=get_job_database(),
                launcher=host_cfg.get('launcher'),
                url=host_cfg.get('host', osmium_cfg['host']),
                jobdir=host_cfg['path'],
                prefix=host_id + '-',
                max_time=host_cfg.get('max_time'))
        elif host_cfg['method'] == 'xenon':
            if not xenon_support:
                raise EnvironmentError("Xenon not installed. Install "
                                       "with pip install -U '.[xenon]'")
            return XenonAdaptor(
                database=get_job_database(),
                host=host_cfg['host'],
                jobdir=host_cfg['path'],
                prefix=host_id + '-',
                max_time=host_cfg.get('max_time', 1440),
                properties=host_cfg)
        else:
            raise EnvironmentError('Connection method for %s unknown' %
                                   host_id)
    except KeyError:
        raise EnvironmentError(
            "Connection method for %s not well configured" % host_id)


def get_host_config(section=None, host_id=None):
    """
    Get the SIM-CITY host_config for a given section in the configuration or
    host_id. At least one of host_id or section must be provided.
    @param section: name of the section in the SIM-CITY configuration
    @param host_id: host ID in the SIM-CITY configuration
    @return: tuple (host_id, host_cfg)
    @raise ValueError: if section and host_id are both None
    """
    if host_id is None and section is None:
        raise ValueError('Specify host or section')
    elif host_id is None:
        host_id = section[:-5] if section.endswith('-host') else section
    elif section is None:
        section = host_id + '-host'

    try:
        return host_id, get_config().section(section)
    except KeyError:
        raise ValueError('{0} not configured under {1} section'
                         .format(host_id, section))


def submit(host_id, adaptor=None):
    """
    Submit a new job to given host.

    @param host_id: given host ID in the SIM-CITY configuration
    @param adaptor: use a custom adaptor instead of reading it from the
        configuration
    @raise EnvironmentError: SIM-CITY configuration does not have host_id fully
        configured
    @return: submitted Job
    """
    host_id, host_cfg = get_host_config(host_id=host_id)

    if adaptor is None:
        adaptor = create_adaptor(host_id, host_cfg)

    try:
        script = [host_cfg['script']] + host_cfg.get('arguments', '').split()
    except KeyError:
        raise EnvironmentError(
            "Connection method for %s not well configured" % host_id)

    return adaptor.submit(script)


def kill(job, adaptor=None):
    """
    Stop a running job.

    @param job: given Job with a 'host_section' attribute.
    @param adaptor: use a custom adaptor instead of reading it from the
        configuration
    @raise ValueError: job has not set 'host_section'
    @raise EnvironmentError: SIM-CITY configuration does not have host_section
        fully configured
    @return: the updated Job object
    """
    try:
        section = job['host_section']
    except KeyError:
        raise ValueError('Job has no host_section set')

    host_id, host_cfg = get_host_config(section=section)

    if adaptor is None:
        adaptor = create_adaptor(host_id, host_cfg)

    return adaptor.kill(job)


def status(jobs, adaptor=None):
    """
    Get the status of a list of jobs.

    @param jobs: given list of Job classes with a 'host_section' attribute.
    @param adaptor: use a custom adaptor instead of reading it from the
        configuration
    @raise ValueError: a Job has not set 'host_section'
    @raise EnvironmentError: SIM-CITY configuration does not have host_section
        fully configured
    @return: a status list in the same order as the input list. Values are one
        of Adaptor.{DONE, RUNNING, PENDING} or None if unknown.
    """
    sections = {}
    try:
        for job in jobs:
            sections.setdefault(job['host_section'], []).append(job)
    except KeyError:
        raise ValueError('Job has no host_section set')

    result = [None]*len(jobs)
    idx = {job.id: i for i, job in enumerate(jobs)}
    for section in sections:
        host_id, host_cfg = get_host_config(section=section)

        if adaptor is None:
            adaptor = create_adaptor(host_id, host_cfg)

        job_status = zip(sections[section], adaptor.status(sections[section]))
        for job, job_result in job_status:
            result[idx[job.id]] = job_result

    return result


class Adaptor(object):
    """ Submits a job """
    PENDING, RUNNING, DONE = 'PENDING', 'RUNNING', 'DONE'

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
        job = queue_job(Job({'_id': job_id}), self.method,
                        host=self.host, database=self.database)
        job['host_section'] = self.prefix + 'host'
        try:
            job['batch_id'] = self._do_submit(job, command)
        except:
            archive_job(job, database=self.database)
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

    def kill(self, job):
        """
        Stop a started job.

        Override in subclasses.
        @return: Job if it can be killed or None if not
        @raise IOError: Adaptor tried to kill Job but failed
        @raise ValueError: if the Job object was submitted with a different
            adaptor.
        """
        raise NotImplementedError

    def check_job(self, job):
        """
        Check whether a job was submitted with the current adaptor
        @param job: Job object
        @raise ValueError: if the Job object was submitted with a different
            adaptor.
        """
        if job.get('host_section') != self.prefix + 'host':
            raise ValueError('Incompatible hosts')
        if 'batch_id' not in job:
            raise ValueError('Cannot kill job without job ID')

    def status(self, jobs):
        """
        Get the status of a list of running jobs

        Override in subclasses.
        @raise ValueError: if one of the Job objects was submitted with a
            different adaptor.
        @param jobs: list of jobs
        @return: list of Adaptor.{DONE, RUNNING, PENDING} or None if impossible
            to know
        """
        raise NotImplementedError


class OsmiumAdaptor(Adaptor):
    """ Submits a job to Osmium. """
    def __init__(self, database, url, prefix, launcher, jobdir="~",
                 max_time=None):
        super(OsmiumAdaptor, self).__init__(
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
            err = IOError("Request failed " + response.reason +
                          "(HTTP status " + response.status + ")",
                          response.status)
            err.status = response.status
            raise err

        return response

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        request = {
            "prestaged": [],
            "poststaged": [],
            'executable':  command[0],
            'arguments':   command[1:],
            'jobdir':      self.jobdir,
            'environment': {'SIMCITY_JOBID': job.id},
        }
        if self.launcher is not None:
            request['launcher'] = self.launcher
        if self.max_time is not None:
            request['max_time'] = int(self.max_time)

        response = self._request(method="POST", data=request)
        return response.location.split('/')[-1]

    def status(self, jobs):
        """ Get the status dict of a single job from Osmium. """
        job_status = []
        for job in jobs:
            self.check_job(job)

            single_status = None
            try:
                response = self._request('/job/{0}'.format(job['batch_id']))
                value = json.loads(response.data)
                if value['status']['running']:
                    single_status = Adaptor.RUNNING
                elif value['status']['done']:
                    single_status = Adaptor.DONE
                else:
                    single_status = Adaptor.PENDING
            except IOError:
                pass

            job_status.append(single_status)

        return job_status

    def kill(self, job):
        """
        Stop a started job.
        """
        self.check_job(job)
        try:
            self._request('/job/{0}'.format(job['batch_id']), method='DELETE')
            return archive_job(job)
        except IOError:
            return None


class SSHAdaptor(Adaptor):
    """ Submits a job over SSH, remotely running the Torque qsub utility. """

    def __init__(self, database, host, prefix, jobdir="~"):
        super(SSHAdaptor, self).__init__(
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

    def kill(self, job):
        """ Cannot stop job, always returns None. """
        self.check_job(job)
        return None

    def status(self, jobs):
        """ Cannot check status of jobs, always returns list of None. """
        for job in jobs:
            self.check_job(job)
        return [None]*len(jobs)


class XenonAdaptor(Adaptor):
    """ Submits job using Xenon. """
    xenon_init = False

    def __init__(self, database, host, prefix, jobdir, max_time=1440,
                 properties=None):
        super(XenonAdaptor, self).__init__(database, host, prefix, jobdir,
                                           "xenon")
        XenonAdaptor.init()
        self.max_time = max_time
        urlsplit = self.host.split('://')
        if len(urlsplit) != 2:
            raise ValueError("host must contain a scheme and a "
                             "hostname, syntax `scheme://host`.")
        self.scheme, self.hostname = urlsplit
        if properties is not None:
            self.private_key = properties.get('private-key')
            self.password = properties.get('password')

            self.xenon_properties = xenon.conversions.dict_to_HashMap({
                key[len('xenon-property-'):]: value
                for key, value in properties.items()
                if key.startswith('xenon-property-')
            })

            self.scheduler_properties = xenon.conversions.dict_to_HashMap({
                key[len('scheduler-property-'):]: value
                for key, value in properties.items()
                if key.startswith('scheduler-property-')
            })
        else:
            self.xenon_properties = None
            self.scheduler_properties = None
            self.private_key = None
            self.password = None

    @staticmethod
    def init(log_level='INFO', **kwargs):
        """ Initialize Xenon. The method is a no-op after the first call. """
        if not XenonAdaptor.xenon_init:
            XenonAdaptor.xenon_init = True
            xenon.init(log_level=log_level, **kwargs)

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        with xenon.Xenon(self.xenon_properties) as x:
            try:
                jobs = x.jobs()
                desc = xenon.jobs.JobDescription()
                desc.addEnvironment('SIMCITY_JOBID', job.id)
                desc.setMaxTime(int(self.max_time))
                desc.setWorkingDirectory(self.jobdir)
                desc.setStdout("stdout_" + job.id + ".txt")
                desc.setStderr("stderr_" + job.id + ".txt")

                scheduler = self.scheduler(x)

                if scheduler.isOnline():
                    desc.setExecutable("/bin/sh")
                    if len(command) == 1:
                        command_str = "'{0}'".format(command[0])
                    else:
                        command_str = "'{0}' '{1}'".format(
                            command[0], "' '".join(command[1:]))
                    desc.setArguments([
                        "-c", "nohup {0} >'{1}' 2>'{2}' &"
                        .format(command_str, desc.getStdout(),
                                desc.getStderr())])
                    job = jobs.submitJob(scheduler, desc)
                    print("Waiting for submission to finish...")
                    jobs.waitUntilDone(job, 0)
                    print("Done.")
                else:
                    desc.setExecutable(command[0])
                    desc.setArguments(command[1:])
                    job = jobs.submitJob(scheduler, desc)

                return job.getIdentifier()
            except xenon.exceptions.XenonException as ex:
                raise IOError(ex.javaClass(),
                              "Cannot submit job with Xenon: {0}"
                              .format(ex.message()))

    def kill(self, job):
        """
        Stop a started job.
        """
        self.check_job(job)

        with xenon.Xenon(self.xenon_properties) as x:
            try:
                for xjob in self.jobs(x):
                    if xjob.getIdentifier() == job['batch_id']:
                        x.jobs().cancelJob(xjob)
                        return archive_job(job)
            except xenon.exceptions.XenonException as ex:
                raise IOError(ex.javaClass(),
                              "Cannot kill job with Xenon: {0}"
                              .format(ex.message()))
        return None

    def status(self, jobs):
        """
        Get the status of a list of running jobs, one of:
        Adaptor.{DONE, RUNNING, PENDING}

        @raise ValueError: if job does not contain 'host_section' and
            'batch_id' attributes.
        """
        for job in jobs:
            self.check_job(job)

        with xenon.Xenon(self.xenon_properties) as x:
            xjobs = self.jobs(x)
            return [XenonAdaptor.single_status(job, xjobs, x) for job in jobs]

    def scheduler(self, x):
        """ Get a Xenon Scheduler. """
        credential = None
        if (self.private_key is not None or
                self.password is not None):
            cred = x.credentials()
            if self.password is not None:
                j_str = jpype.JString(self.password)
                password = jpype.java.lang.String(j_str).toCharArray()
            else:
                password = None

            if self.private_key is not None:
                credential = cred.newCertificateCredential(
                    'ssh', self.private_key, None, password,
                    None)
            else:
                credential = cred.newPasswordCredential(
                    'ssh', None, password, None)

        return x.jobs().newScheduler(self.scheme, self.hostname, credential,
                                     self.scheduler_properties)

    def jobs(self, x):
        """ Get a list of Xenon Job objects. """
        return x.jobs().getJobs(self.scheduler(x), [])

    @staticmethod
    def single_status(job, xjobs, x):
        """
        Get the status of a single job, given a list of Xenon Job objects.
        @param job: SIM-CITY Job
        @param xjobs: list of Xenon Job objects
        @param x: Xenon
        @return one of Adaptor.{DONE, RUNNING, PENDING}.
        """
        try:
            for xjob in xjobs:
                if xjob.getIdentifier() == job['batch_id']:
                    xstatus = x.jobs().getJobStatus(xjob)

                    if xstatus.isRunning():
                        return Adaptor.RUNNING
                    elif xstatus.isDone():
                        return Adaptor.DONE
                    else:
                        return Adaptor.PENDING
            return Adaptor.DONE
        except xenon.exceptions.XenonException:
            return None
