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

try:
    from ConfigParser import ConfigParser, NoSectionError
except ImportError:
    from configparser import ConfigParser, NoSectionError

from .util import expandfilenames
import os
import picas


class Config(object):
    """
    Manages configuration, divided up in sections.

    A list of additional configurators can be given as subconfigs. Later
    configurators in the list will overwrite those of earlier ones.
    """
    def __init__(self, subconfigs=None):
        self.subconfigs = subconfigs
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
        """
        values = {}
        for cfg in self.subconfigs:
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
        for cfg in self.subconfigs:
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
            filenames = Config.DEFAULT_FILENAMES

        self.parser = ConfigParser()
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
    def __init__(self, url, database, user=None, password=None):
        """
        Initialize the database connection.

        Parameters
        ----------
        url: URL to the CouchDB database
        auth: tuple of username password, if needed
        defaults: Config for default values
        """
        self.db = picas.CouchDB(url=url, db=database,
                                username=user, password=password)

    def section(self, name):
        try:
            value = self.db.get(name)
        except ValueError:
            raise KeyError()

        return dict_value_expandvar(value.settings)

    def sections(self):
        all_settings = self.db.view('_all_docs', 'settings')
        return frozenset([doc.id for doc in all_settings])


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
