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
import simcity
import pytest


class MockSubmitter(simcity.SubmitAdaptor):
    BATCH_ID = 'my_batch_id'

    def __init__(self, database, do_raise=False, method='local'):
        super(MockSubmitter, self).__init__(
            database, 'nohost', 'myjob', '/', method)
        self.do_raise = do_raise

    def _do_submit(self, job, command):
        if self.do_raise:
            raise IOError
        else:
            return MockSubmitter.BATCH_ID

    def kill(self, job):
        return None

    def status(self, jobs):
        return None


def config_database(db, in_progress, pending, running, pending_jobs):
    db.set_view([
        ('running_jobs', running),
        ('pending_jobs', pending_jobs),
        ('pending', pending),
        ('in_progress', in_progress),
    ])


def _set_host_config(hostname, method='local'):
    cfg = simcity.Config()
    cfg.add_section(hostname + '-host', {
        'script': 'mynonexistingscript.sh',
        'host': 'does_not_exist',
        'port': 50080,
        'path': '~',
        'method': method,
    })
    try:
        simcity.init(cfg)
    except KeyError:
        pass  # ignore mal-configured databases in this config


def test_submit_if_needed_already_active(db):
    config_database(db, 1, 1, 2, 0)
    assert simcity.submit_if_needed('nohost', 2) is None


def test_submit_if_needed_pending(db):
    config_database(db, 0, 1, 0, 1)
    assert simcity.submit_if_needed('nohost', 2) is None


def test_submit_if_needed_maxed(db):
    config_database(db, 2, 5, 2, 0)
    assert simcity.submit_if_needed('nohost', 2) is None


def test_unconfigured(db):
    config_database(db, 0, 1, 0, 0)
    _set_host_config('otherhost')
    pytest.raises(ValueError, simcity.submit_if_needed, 'nohost', 2)


def test_submit_if_needed_notactive(db):
    _set_host_config('nohost')
    config_database(db, 0, 1, 0, 0)
    submitter = MockSubmitter(db)
    job = simcity.submit_if_needed('nohost', 2, adaptor=submitter)
    assert job is not None
    assert job['batch_id'] == MockSubmitter.BATCH_ID
    assert job['hostname'] == 'nohost'
    assert job['method'] == 'local'


def test_submit_error(db, ):
    _set_host_config('nohost')
    config_database(db, 0, 1, 5, 0)
    submitter = MockSubmitter(db, do_raise=True)
    pytest.raises(IOError, simcity.submit, 'nohost', adaptor=submitter)
    for saved in db.saved.keys():
        if saved.startswith('job_myjob') and db.saved[saved]['archive'] > 0:
            break
    else:
        pytest.fail("job not archived")


def test_submit_method_not_configured():
    _set_host_config('nohost')
    pytest.raises(EnvironmentError, simcity.submit, 'nohost')


def test_ssh_submit_method(db):
    _set_host_config('nohost', method='ssh')
    config_database(db, 0, 0, 0, 0)
    pytest.raises(IOError, simcity.submit, 'nohost')


def test_osmium_submit_method(db):
    _set_host_config('nohost', method='osmium')
    config_database(db, 0, 0, 0, 0)
    pytest.raises(IOError, simcity.submit, 'nohost')


@pytest.mark.skipif(not simcity.xenon_support, reason="xenon is not installed")
def test_xenon_submit_method(db):
    # setup host with Xenon torque adaptor
    _set_host_config('nohost', method='xenon')
    cfg = simcity.get_config().section('nohost-host')
    cfg['host'] = 'torque://' + cfg['host']
    simcity.get_config().add_section('nohost-host', cfg)
    config_database(db, 0, 0, 0, 0)
    pytest.raises(IOError, simcity.submit, 'nohost')
