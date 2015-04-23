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
import simcity
from nose.tools import (assert_false, assert_equal, assert_not_equal,
                        assert_raises)
from test_mock import MockDB, MockRow


class MockSubmitter(simcity.Submitter):
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


def _set_database(locked, todo, active, pending):
    db = MockDB(view=[
        MockRow('active_jobs', active),
        MockRow('pending_jobs', pending),
        MockRow('todo', todo),
        MockRow('locked', locked),
    ])
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)
    return db


def _set_host_config(hostname, method='local'):
    cfg = simcity.Config(from_file=False)
    cfg.add_section(hostname + '-host', {
        'script': 'mynonexistingscript.sh',
        'host': 'does_not_exist',
        'port': 50080,
        'path': '~',
        'method': method,
    })
    try:
        simcity.init(cfg)
    except EnvironmentError:
        pass  # ignore mal-configured databases in this config


def test_max_not_number():
    assert_raises(ValueError, simcity.submit_if_needed, 'something', '2')


def test_submit_if_needed_already_active():
    _set_database(1, 1, 1, 0)
    assert_equal(simcity.submit_if_needed('nohost', 2), None)


def test_submit_if_needed_pending():
    _set_database(0, 1, 0, 1)
    assert_equal(simcity.submit_if_needed('nohost', 2), None)


def test_submit_if_needed_maxed():
    _set_database(2, 5, 2, 0)
    assert_equal(simcity.submit_if_needed('nohost', 2), None)


def test_unconfigured():
    _set_database(0, 1, 0, 0)
    assert_raises(ValueError, simcity.submit_if_needed, 'nohost', 2)


def test_submit_if_needed_notactive():
    _set_host_config('nohost')
    db = _set_database(0, 1, 0, 0)
    submitter = MockSubmitter(db)
    job = simcity.submit_if_needed('nohost', 2, submitter=submitter)
    assert_not_equal(job, None)
    assert_equal(job['batch_id'], MockSubmitter.BATCH_ID)
    assert_equal(job['hostname'], 'nohost')
    assert_equal(job['method'], 'local')


def test_submit_if_needed_notreallyactive():
    _set_host_config('nohost')
    db = _set_database(0, 1, 5, 0)
    submitter = MockSubmitter(db)
    job = simcity.submit_if_needed('nohost', 2, submitter=submitter)
    assert_not_equal(job, None)


def test_submit_error():
    _set_host_config('nohost')
    db = _set_database(0, 1, 5, 0)
    submitter = MockSubmitter(db, do_raise=True)
    assert_raises(IOError, simcity.submit, 'nohost', submitter=submitter)
    for saved in db.saved.keys():
        if saved.startswith('archived-job_myjob'):
            break
    else:
        assert_false("job not archived")


def test_submit_method_not_configured():
    _set_host_config('nohost')
    assert_raises(EnvironmentError, simcity.submit, 'nohost')


def test_SSH_submit_method():
    _set_host_config('nohost', method='ssh')
    assert_raises(IOError, simcity.submit, 'nohost')


def test_Osmium_submit_method():
    _set_host_config('nohost', method='osmium')
    assert_raises(IOError, simcity.submit, 'nohost')
