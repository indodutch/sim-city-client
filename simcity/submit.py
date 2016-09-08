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
import requests
import subprocess
from uuid import uuid4

try:
    import xenon
    from xenon import java
    xenon_support = True
except ImportError:
    xenon_support = False


class SubmitHandler(object):
    def __init__(self, config, job_handler):
        self.config = config
        self.job_handler = job_handler

    def adaptor(self, host_id=None, job=None, section=None):
        """
        Create the appropriate adaptor given the host_id and host config.

        @param host_id: host_id as mentioned in the configuration
        @param job: existing job
        @param section: section in the configuration
        @raise EnvironmentError: if host_cfg does not configure all needed
            properties
        @return: SubmitAdaptor
        """
        global xenon_support

        if section is None and job is not None and 'host_section' in job:
            section = job['host_section']

        if host_id is not None or section is not None:
            if host_id is None:
                if section.endswith('-host'):
                    host_id = section[:-5]
                else:
                    host_id = section
            elif section is None:
                section = host_id + '-host'

            host_cfg = self.config.section(section)
        else:
            raise ValueError('No adaptor is specified.')

        try:
            default_command = [host_cfg['script']]
            default_command += host_cfg.get('arguments', '').split()
            if host_cfg['method'] == 'ssh':
                return SSHAdaptor(
                    job_handler=self.job_handler,
                    host=host_cfg['host'],
                    default_command=default_command,
                    jobdir=host_cfg['path'],
                    prefix=host_id + '-')
            elif host_cfg['method'] == 'osmium':
                try:
                    osmium_cfg = self.config.section('osmium')
                except KeyError:
                    osmium_cfg = {'host': 'localhost:9998'}

                return OsmiumAdaptor(
                    job_handler=self.job_handler,
                    launcher=host_cfg.get('launcher'),
                    url=host_cfg.get('host', osmium_cfg['host']),
                    default_command=default_command,
                    jobdir=host_cfg['path'],
                    prefix=host_id + '-',
                    max_time=host_cfg.get('max_time'))
            elif host_cfg['method'] == 'xenon':
                if not xenon_support:
                    raise EnvironmentError(
                        "Xenon not installed. Install with "
                        "pip install -U 'simcity[xenon]'")
                return XenonAdaptor(
                    job_handler=self.job_handler,
                    host=host_cfg['host'],
                    default_command=default_command,
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

    def status(self, jobs):
        """
        Get the status of a list of jobs.

        @param jobs: given list of Job classes with a 'host_section' attribute.
        @raise ValueError: a Job has not set 'host_section'
        @raise EnvironmentError: SIM-CITY configuration does not have host_section
            fully configured
        @return: a status list in the same order as the input list. Values are one
            of SubmitAdaptor.{DONE, RUNNING, PENDING} or None if unknown.
        """
        sections = {}
        try:
            for job in jobs:
                sections.setdefault(job['host_section'], []).append(job)
        except KeyError:
            raise ValueError('Job has no host_section set')

        result = [None] * len(jobs)
        idx = {job.id: i for i, job in enumerate(jobs)}
        for section in sections:
            adaptor = self.adaptor(section=section)

            job_status = zip(sections[section],
                             adaptor.status(sections[section]))
            for job, job_result in job_status:
                result[idx[job.id]] = job_result

        return result


class SubmitAdaptor(object):
    """ Submits a job """
    PENDING, RUNNING, DONE = 'PENDING', 'RUNNING', 'DONE'

    def __init__(self, job_handler, host, prefix, jobdir, method,
                 default_command):
        self.job_handler = job_handler
        self.host = host
        self.jobdir = jobdir
        self.prefix = prefix
        self.method = method
        self.default_command = default_command

    def submit(self, command=None):
        """
        Submit given command to the configured host.

        Raises IOError if the command could not be submitted.
        """
        if command is None:
            command = self.default_command

        job_id = 'job_' + self.prefix + uuid4().hex
        job = self.job_handler.prepare_for_queue(
            Job({'_id': job_id}), self.method, self.prefix + 'host', self.host)
        try:
            batch_id = self._do_submit(job, command)
        except:
            self.job_handler.archive(job)
            raise
        else:
            return self.job_handler.queued(job, batch_id)

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
        @raise IOError: SubmitAdaptor tried to kill Job but failed
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
        @return: list of SubmitAdaptor.{DONE, RUNNING, PENDING} or None if impossible
            to know
        """
        raise NotImplementedError


class OsmiumAdaptor(SubmitAdaptor):
    """ Submits a job to Osmium. """
    def __init__(self, job_handler, url, prefix, launcher, default_command,
                 jobdir="~", max_time=None):
        super(OsmiumAdaptor, self).__init__(
            job_handler, url, prefix, jobdir, "osmium", default_command)
        self.launcher = launcher
        self.max_time = max_time

    def _request(self, location="/", method="GET", data=None):
        """ Make a HTTPS request to the Osmium service. """
        url = 'http://{0}{1}'.format(self.host, location)
        response = requests.request(method, url, json=data)
        if response.status_code < 200 or response.status_code >= 300:
            err = IOError("Request failed " + response.reason +
                          "(HTTP status " + response.status_code + ")",
                          response.status_code)
            err.status = response.status_code
            raise err

        return response

    def _do_submit(self, job, command):
        """ Submit a command with given job metadata. """
        request = {
            "prestaged": [],
            "poststaged": [],
            'executable': command[0],
            'arguments': command[1:],
            'jobdir': self.jobdir,
            'environment': {'SIMCITY_JOBID': job.id},
        }
        if self.launcher is not None:
            request['launcher'] = self.launcher
        if self.max_time is not None:
            request['max_time'] = int(self.max_time)

        response = self._request(method="POST", data=request)
        return response.headers['location'].split('/')[-1]

    def status(self, jobs):
        """ Get the status dict of a single job from Osmium. """
        job_status = []
        for job in jobs:
            self.check_job(job)

            single_status = None
            try:
                response = self._request('/job/{0}'.format(job['batch_id']))
                value = response.json()
                if value['status']['running']:
                    single_status = SubmitAdaptor.RUNNING
                elif value['status']['done']:
                    single_status = SubmitAdaptor.DONE
                else:
                    single_status = SubmitAdaptor.PENDING
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
            return self.job_handler.archive(job)
        except IOError:
            return None


class SSHAdaptor(SubmitAdaptor):
    """ Submits a job over SSH, remotely running the Torque qsub utility. """

    def __init__(self, job_handler, host, prefix, default_command, jobdir="~"):
        super(SSHAdaptor, self).__init__(
            job_handler, host, prefix, jobdir, "ssh", default_command)

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
        return [None] * len(jobs)


class XenonAdaptor(SubmitAdaptor):
    """ Submits job using Xenon. """
    def __init__(self, job_handler, host, prefix, default_command, jobdir,
                 max_time=1440, properties=None):
        super(XenonAdaptor, self).__init__(job_handler, host, prefix, jobdir,
                                           "xenon", default_command)
        try:
            xenon.init(log_level='INFO')
        except ValueError:
            pass  # xenon is already initialized

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
                        return self.job_handler.archive(job)
            except xenon.exceptions.XenonException as ex:
                raise IOError(ex.javaClass(),
                              "Cannot kill job with Xenon: {0}"
                              .format(ex.message()))
        return None

    def status(self, jobs):
        """
        Get the status of a list of running jobs, one of:
        SubmitAdaptor.{DONE, RUNNING, PENDING}

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
                password = java.lang.String(self.password).toCharArray()
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
        @return one of SubmitAdaptor.{DONE, RUNNING, PENDING}.
        """
        try:
            for xjob in xjobs:
                if xjob.getIdentifier() == job['batch_id']:
                    xstatus = x.jobs().getJobStatus(xjob)

                    if xstatus.isRunning():
                        return SubmitAdaptor.RUNNING
                    elif xstatus.isDone():
                        return SubmitAdaptor.DONE
                    else:
                        return SubmitAdaptor.PENDING
            return SubmitAdaptor.DONE
        except xenon.exceptions.XenonException:
            return None
