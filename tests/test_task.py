from nose.tools import assert_equal, assert_true
from test_mock import MockDB
import simcity


def test_add_task():
    simcity.management._task_db = MockDB()
    task = simcity.add_task({'key': 'my value'})
    assert_equal(task['key'], 'my value')
    assert_true(len(task.id) > 0)
