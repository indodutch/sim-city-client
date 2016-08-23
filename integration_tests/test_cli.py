import simcity
from simcity.__main__ import fill_argument_parser
import argparse
import time

test_config = 'integration_tests/docker/config.ini'


def test_init():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'init', '-u', 'simcityadmin',
                              '-p', 'simcity'])
    args.func(args)
    # now views work
    assert 0 == sum(simcity.overview_total().values())
    simcity.add_task({'command': 'echo', 'arguments': ['Hello world 1']})
    assert 1 == sum(simcity.overview_total().values())


def test_create():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'create',
                              'echo', 'Hello world 2'])
    args.func(args)

    pending_rows = simcity.get_task_database().view('pending').rows
    assert 2 == len(pending_rows)
    for row in pending_rows:
        assert 0 == row.value['lock']
        assert 0 == row.value['done']
        task = simcity.get_task(row.id)
        assert task['command'] == 'echo'
        assert task['arguments'][0].startswith('Hello world')

    simcity.add_task({'command': 'sleep', 'arguments': ['5']})


def test_submit():
    parser = argparse.ArgumentParser()
    fill_argument_parser(parser)
    args = parser.parse_args(['-c', test_config, 'submit', 'slurm'])
    args.func(args)
    totals = simcity.overview_total()
    assert 1 == totals['pending_jobs'] + totals['running_jobs']
    assert 1 == totals['active_jobs']

    time.sleep(3)
    totals = simcity.overview_total()
    assert 1 == totals['running_jobs']
    assert 1 <= totals['in_progress']
    time.sleep(3)
    totals = simcity.overview_total()
    assert 3 == totals['done']
    assert 0 == totals['in_progress']
    assert 0 == totals['error']
    assert 1 == totals['finished_jobs']
    assert 1 == totals['archived_jobs']

    job = None
    for row in simcity.get_job_database().view('archived_jobs'):
        job = simcity.get_job(row.id)
        assert 1 == job['parallelism']

    assert job is not None

    task = None
    for row in simcity.get_task_database().view('done'):
        task = simcity.get_task(row.id)
        assert 1 == task['parallelism']
        assert len(task['execute_properties']['env']) > 5
        assert len(task['files']) == 2
        assert job.id == task['job']

    assert task is not None
