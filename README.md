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

Simply run

    pip install .

Copy the `config.ini.dist` file to `config.ini` or to `~/.simcity_client`. See the comments therein for more information. Set the correct values for the CouchDB database and if you intend to run jobs on this location also set the executable settings. There are two CouchDB database sections, one for the jobs and one for the tasks. If these are the same, you can remove the jobs database section. If you anticipate large output files, you can configure a webdav url that the files can be stored on.

## Usage

**Run a script** on a cluster:

    $ python scripts/submitJob.py 'path/to/job/on/cluster' cluster

The cluster can be configured in the config.ini file, as a section [$CLUSTER_NAME-host].

**Load tasks** to your database: 

	$ python scripts/createTasks.py COMMAND TOKEN_ID

The TOKEN_ID must be unique. Refresh the database to see the tasks.

**Create basic views** (todo, locked, done, error, overview):

	$ python scripts/createViews.py

Refresh the database to see the views. Unfold top right tab 'View:All documents' to inspect each view.

**Run the client** to process tokens in the database:
   
	$ python scripts/run.py

This will run an executable on your local machine. To submit the same on a cluster, you need to add the 'python run.py' command for example in a shell script and submit this with qsub. See for example `scripts/lisaSubmitExpress.sh`.

## API

The API consists of all methods exported in `__init__.py`. It assumes that a configuration file is available to set the databases and execution options with. The `simcity.init(configfile)` must be called before any other `simcity` function.

## Contributing

To add a feature or bug-fix, create a new clone and/or branch. Install testing tools with

    pip install -r requirements.txt '.[testing]'

And run

    nosetests

before every commit. When you're done, make a pull request on GitHub.
