# Run a simulation on SIM-CITY infrastructure

Uses a pilot job script to run multiple simulations in the same job.

## Dependencies

The best way to manage this package is through the Python `virtualenv` package. On a cluster resource run the following commands:

    module load python/2.7
    pip install --user virtualenv
    ~/.local/bin/virtualenv simcity

Then before every run or installation, include the `simcity` virtualenv:

    module load python/2.7
    source simcity/bin/activate

Install Python `picas`:

	git clone https://github.com/blootsvoets/picasclient.git picas
    cd picas
    git checkout setup
    pip install .

## Installation

Simply run

    make install

Copy the `config.ini.dist` file to `config.ini` or to `~/.simcity_client`. Set the correct values for the CouchDB database and executable settings.

## Usage

**Load tokens** to your database: 

	$ python scripts/createTokens.py

Refresh the database to see the tokens

**Create basic views** (todo, locked, done, overview):

	$ python scripts/createViews.py

Refresh the database to see the views. Unfold top right tab 'View:All documents' to inspect each view.

**Run a simple example** that calculates the square of a set of tokens:
   
	$ python scripts/runExample.py
   
This will run the example in your local machine. To submit the same on a cluster, you need to add the 'python runExample.py' command for example in a shell script and submit this with qsub.

## Contributing

To commit a change git, first run

    make test

and then commit, to make sure the change didn't break any code.
