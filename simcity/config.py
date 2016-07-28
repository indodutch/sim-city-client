# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
""" Configurators for the SIM-CITY client. """
try:
    from ConfigParser import RawConfigParser, NoSectionError
except ImportError:
    from configparser import RawConfigParser, NoSectionError

from .util import expandfilenames
from .database import CouchDB
import os


class Config(object):
    """
    Manages configuration, divided up in sections.

    A list of additional configurators contains the actual configuration. Later
    configurators in the list will overwrite those of earlier ones.
    Configurators must implement the section() and sections() methods.
    """
    def __init__(self, configurators=None):
        if configurators is None:
            configurators = []
        self.configurators = configurators
        self._sections = {}

    def add_section(self, name, keyvalue):
        """
        Add (overwrite) the configuration of a section.

        Parameters
        ----------
        name : str
            section name
        keyvalue : dict
            keys with values of the section
        """
        self._sections[name] = dict_value_expandvar(keyvalue)

    def section(self, name):
        """ Get the dict of key-values of config section.
        @param name: section name. Use 'DEFAULT' for default (unnamed) section.
        @raise KeyError: if section does not exist
        """
        values = {}
        for cfg in self.configurators:
            try:
                values.update(cfg.section(name))
            except KeyError:
                pass

        try:
            values.update(self._sections[name])
        except KeyError:
            if len(values) == 0:
                raise  # otherwise, it was found in one of the sub-configs.

        return values

    def sections(self):
        """ The set of configured section names. """
        sections = set(self._sections.keys())
        for cfg in self.configurators:
            sections |= cfg.sections()

        return sections


class FileConfig(object):
    """
    Manages configuration, divided up in sections.

    Configuration can be read from a python config or ini file. Those files are
    divided into sections, where the first entries fall in the DEFAULT section.
    Within each section, entries are stored as key-value pairs with unique
    keys.
    """
    DEFAULT_FILENAMES = [
        "config.ini", ("..", "config.ini"), ("~", ".simcity_client")]

    def __init__(self, filenames=None):
        if filenames is None:
            filenames = FileConfig.DEFAULT_FILENAMES

        self.parser = RawConfigParser()
        self.filename = self.parser.read(expandfilenames(filenames))
        if len(self.filename) == 0:
            raise ValueError(
                "No valid configuration files could be found: tried " +
                str(expandfilenames(filenames)))

    def section(self, name):
        """ Get key-values of a config section.
        @param name: str name of the config section
        @return: dict of key-values
        """
        try:
            return dict_value_expandvar(dict(self.parser.items(name)))
        except NoSectionError:
            raise KeyError()

    def sections(self):
        """ The set of configured section names, including DEFAULT. """
        return frozenset(['DEFAULT'] + self.parser.sections())


class CouchDBConfig(object):
    """
    Reads configuration from a database, divided up in sections.

    Configuration can be read from a python config or ini file. Those files are
    divided into sections, where the first entries fall in the DEFAULT section.
    Within each section, entries are stored as key-value pairs with unique
    keys.
    """
    def __init__(self, database, sections_view='_all_docs',
                 sections_design_docs='Settings'):
        """
        Initialize the database connection.

        Parameters
        ----------
        database: CouchDB object
        sections_view: view name that lists all sections
        sections_design_docs: design document of sections_view
        """
        self.db = database
        self.sections_view = sections_view
        self.sections_design_docs = sections_design_docs

    def section(self, name):
        """ Get key-values of a config section.
        @param name: str name of the config section
        @return: dict of key-values """
        try:
            value = self.db.get(name)
        except ValueError:
            raise KeyError()

        return dict_value_expandvar(value['settings'])

    def sections(self):
        """ The set of configured section names. """
        all_settings = self.db.view(self.sections_view,
                                    design_doc=self.sections_design_docs)
        return frozenset([doc.id for doc in all_settings])

    @classmethod
    def from_url(cls, url, database, user=None, password=None, **kwargs):
        """ Create a CouchDBConfig from a CouchDB URL and database name.
        Additional arguments are passed to __init__."""
        return cls(database=CouchDB(url=url, db=database,
                                    username=user,
                                    password=password), **kwargs)


def dict_value_expandvar(d):
    """ Expand all environment variables of given dict.
        Uses os.path.expandvars internally. Only applies to str values.
        @param d: dict to expand values of. """

    for key in d:
        try:
            d[key] = os.path.expandvars(d[key])
        except TypeError:
            pass  # d[key] is not a string
    return d
