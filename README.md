# Run a simulation on SIM-CITY infrastructure

The best way to manage this package is through the virtualenv package. On a cluster resource run the following commands:

    module load python/2.7
    pip install --user virtualenv
    ~/.local/bin/virtualenv simcity

Then before every run, include the simcity virtualenv:

    module load python/2.7
    source simcity/bin/activate

## Dependencies

Install Python CouchDB and picasclient:

	git clone https://github.com/blootsvoets/picasclient.git picas
    cd picas
    git checkout setup
    pip install .
    cd ..
	pip install .

## Usage

**Load tokens** to your database: 

	$ python createTokens.py [picas_db_name] [picas_username] [picas_pwd]

Refresh the database to see the tokens

**Create basic views** (todo, locked, done, overview):

	$ python createViews.py [picas_db_name] [picas_username] [picas_pwd]

Refresh the database to see the views. Unfold top right tab 'View:All documents' to inspect each view.

**Run a simple example** that calculates the square of a set of tokens:
   
	$ python runExample.py [picas_db_name] [picas_username] [picas_pwd]
   
This will run the example in your local machine. To submit the same on a cluster, you need to add the 'python runExample.py' command for example in a shell script and submit this with qsub.
