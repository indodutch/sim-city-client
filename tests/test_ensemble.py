from __future__ import print_function

import simcity
from nose.tools import assert_equals, assert_true
from test_mock import MockDB


def test_create_ensemble_view():
    simcity.management._reset_globals()
    db = MockDB()
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)

    url = 'http://fun.host/mydb'
    db.url = url

    result = simcity.ensemble_view(db, 'mysim', '0.1')

    assert_equals(1, len(db.views))
    assert_true('all_docs' in db.views)
    assert_equals('mysim_0.1', result)
    assert_equals('mysim_0.1', db.views['all_docs']['design'])
    assert_true(len(db.views['all_docs']['map']) > 0)
    assert_true(url in db.views['all_docs']['map'])


def test_existing_ensemble_view():
    simcity.management._reset_globals()
    db = MockDB()
    simcity.management.set_task_database(db)
    simcity.management.set_job_database(db)

    db.save(simcity.Document({'_id': '_design/mysim_0.1'}))

    result = simcity.ensemble_view(db, 'mysim', '0.1')

    assert_equals('mysim_0.1', result)
    assert_equals(0, len(db.views))
