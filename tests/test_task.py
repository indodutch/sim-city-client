from nose.tools import assert_equal, assert_true
from test_mock import MockDB
import simcity


def test_add_task():
    simcity.management.set_task_database(MockDB())
    task = simcity.add_task({'key': 'my value'})
    assert_equal(task['key'], 'my value')
    assert_true(len(task.id) > 0)


def test_get_task():
    simcity.management.set_task_database(MockDB())
    task = simcity.get_task(MockDB.TASKS[0]['_id'])
    assert_equal(task.id, MockDB.TASKS[0]['_id'])
