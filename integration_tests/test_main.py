import simcity
from simcity.__main__ import fill_argument_parser
import argparse
from nose.tools import assert_equals, assert_true
import time

test_config = 'integration_tests/docker/config.ini'


def test_init():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'init', '-u', 'simcityadmin',
                              '-p', 'simcity'])
    args.func(args)
    # now views work
    assert_equals(0, sum(simcity.overview_total().values()))
    simcity.add_task({'command': 'echo', 'arguments': ['Hello world 1']})
    assert_equals(1, sum(simcity.overview_total().values()))


def test_create():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'create',
                              'echo', 'Hello world 2'])
    args.func(args)

    pending_rows = simcity.get_task_database().view('pending').rows
    assert_equals(2, len(pending_rows))
    for row in pending_rows:
        assert_equals(0, row.value['lock'])
        assert_equals(0, row.value['done'])
        task = simcity.get_task(row.id)
        assert_equals(task['command'], 'echo')
        assert_true(task['arguments'][0].startswith('Hello world'))

    simcity.add_task({'command': 'sleep', 'arguments': ['5']})


def test_submit():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'submit', 'slurm'])
    args.func(args)
    totals = simcity.overview_total()
    assert_equals(1, totals['pending_jobs'] + totals['running_jobs'])
    assert_equals(1, totals['active_jobs'])

    time.sleep(3)
    totals = simcity.overview_total()
    assert_equals(1, totals['running_jobs'])
    assert_true(1 <= totals['in_progress'])
    time.sleep(3)
    totals = simcity.overview_total()
    assert_equals(3, totals['done'])
    assert_equals(0, totals['in_progress'])
    assert_equals(0, totals['error'])
    assert_equals(1, totals['finished_jobs'])
    assert_equals(1, totals['archived_jobs'])

    job_id = next(simcity.get_job_database().view('archived_jobs')).id
    job = simcity.get_job(job_id)
    assert_equals(1, job['parallelism'])

    task_id = next(simcity.get_task_database().view('done')).id
    task = simcity.get_task(task_id)
    assert_equals(1, task['parallelism'])
    assert_true(len(task['execute_properties']['env']) > 5)
    assert_true(len(task['files']) == 2)
    assert_equals(job.id, task['job'])
