from simcity_client import util, database
from ConfigParser import NoSectionError

def init_couchdb():
    try:
        config = util.Config()
        return (config, database.CouchDB(config.section('CouchDB')))
    except NoSectionError:
        raise ValueError("Configuration file " + config.filename + "does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB: " + ex)