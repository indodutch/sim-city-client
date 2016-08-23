from __future__ import print_function

import simcity


def test_create_ensemble_view(db):
    url = 'http://fun.host/mydb'
    db.url = url

    result = simcity.ensemble_view(db, 'mysim', '0.1')

    assert 1 == len(db.views)
    assert 'all_docs' in db.views
    assert 'mysim_0.1' == result
    assert 'mysim_0.1' == db.views['all_docs']['design']
    assert len(db.views['all_docs']['map']) > 0
    assert url in db.views['all_docs']['map']


def test_existing_ensemble_view(db):
    db.save(simcity.Document({'_id': '_design/mysim_0.1'}))

    result = simcity.ensemble_view(db, 'mysim', '0.1')

    assert 'mysim_0.1' == result
    assert 0 == len(db.views)
