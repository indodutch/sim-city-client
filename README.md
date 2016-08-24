# Run a simulation on SIM-CITY infrastructure

[![Build Status](https://travis-ci.org/indodutch/sim-city-client.svg?branch=master)](https://travis-ci.org/indodutch/sim-city-client)
[![Codacy Grade](https://api.codacy.com/project/badge/grade/60c3365bb4ad43aeba99954ac8a85433)](https://www.codacy.com/app/github_4/sim-city-client)
[![Codacy Coverage](https://api.codacy.com/project/badge/coverage/60c3365bb4ad43aeba99954ac8a85433)](https://www.codacy.com/app/github_4/sim-city-client)

SIM-CITY client uses a pilot job script to run multiple simulations in the same job. Open source under the Apache License Version 2.0. For installing the rest of the SIM-CITY infrastructure, see the [wiki](https://github.com/indodutch/sim-city-client/wiki).

## Dependencies

The best way to manage this package is through the Python `virtualenv` package, with `pip` version 7 or higher. On a cluster resource run the following commands:

    module load python/2.7
    pip install --user virtualenv
    ~/.local/bin/virtualenv simcity

Then before every run or installation, include the `simcity` virtualenv:

    module load python/2.7
    . simcity/bin/activate

## Installation

If you want to submit to a cluster from the local host, run

    pip install -U ".[xenon]"

Otherwise, and on the cluster itself, run

    pip install -U .

Copy the `config.ini.dist` file to `config.ini` or to `~/.simcity_client`. See the comments therein for more information. Set the correct values for the CouchDB database and if you intend to run jobs on this location also set the executable settings. There are two CouchDB database sections, one for the jobs and one for the tasks. If these are the same, you can remove the jobs database section. If you anticipate large output files, you can configure a webdav url that the files can be stored on.

## Usage

All scripts are available with the `simcity` command. Run `simcity --help` and
`simcity [SUBCOMMAND] --help` to see the options.

**Create the CouchDB database and views** by running

    simcity init [CouchDB admin username]

If the database already exists, only create the necessary views with

    simcity init --view

**Create a new task or simulation** by first creating the input parameters in
`input.json`:

```json
{
    "x": 1,
    "y": 2,
    "simulation": "2d-game",
    "ensemble": "first-exploration"
}
```
and creating the task

    simcity create -i input.json 'path/to/2d-game.sh' arg1 arg2

**Run the simulation locally** with

    simcity run --local 

**Run the simulation on the cluster** by installing SIM-CITY client there with the same configuration as locally. Then locally add the cluster configuration in `config.ini`, as a section `[CLUSTER_NAME-host]` (see `config.ini.dist` for some examples.) This requires a script on that cluster that will start `simcity run` in the correct virtualenv. For example see `scripts/lisaRun.sh` for a script to submit with configuration `method = xenon`.

Then run the following to submit a job to the cluster:

    simcity submit CLUSTER_NAME

To check the status of the jobs and tasks, clean up the database, and start jobs as necessary, run

    simcity check CLUSTER_NAME

Use the `-n` flag to only list the actions that `check` would perform.

To view tasks that are in progress, for example, run

    simcity list in_progress

or tasks that are done

    simcity list done

To get one such task, get it with

    simcity get mytaskid

or download all its attachments with

    simcity get --download path/to/results mytaskid 

## API

The API consists of all methods exported in `__init__.py`. It assumes that a configuration file is available to set the databases and execution options with. The `simcity.init(configfile)` must be called before any other `simcity` function.

## Contributing

To add a feature or bug-fix, create a new clone and/or branch. First make sure Python versions 2.7 and 3.5 are installed as well as `docker-compose`. Then install testing tools with

    pip install tox

And run

    tox

When you're done, make a pull request on GitHub.
