Run a simulation on infrastructure

## Dependencies

Install Python CouchDB and picasclient:

	pip install couchdb
	git clone https://github.com/jjbot/picasclient.git picas

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
