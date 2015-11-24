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

""" Utility functions. """

try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

import json
import os
import glob
import shutil
from numbers import Number


class Config(object):
    """
    Manages configuration, divided up in sections.

    Configuration can be read from a python config or ini file. Those files are
    divided into sections, where the first entries fall in the DEFAULT section.
    Within each section, entries are stored as key-value pairs with unique keys.
    """
    DEFAULT_FILENAMES = [
        "config.ini", ("..", "config.ini"), ("~", ".simcity_client")]

    def __init__(self, filenames=None, from_file=True):
        if from_file:
            if filenames is None:
                filenames = Config.DEFAULT_FILENAMES

            exp_filenames = expandfilenames(filenames)

            self.parser = ConfigParser()
            self.filename = self.parser.read(exp_filenames)
            if len(self.filename) == 0:
                raise ValueError(
                    "No valid configuration files could be found: tried " +
                    str(exp_filenames))
        else:
            self.parser = None
            self.filename = None

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
        """ Get the dict of key-values of config section. """
        try:
            return self._sections[name]
        except KeyError:
            if self.parser is not None and (name == 'DEFAULT' or
                                            self.parser.has_section(name)):
                return dict_value_expandvar(dict(self.parser.items(name)))
            else:
                raise

    def sections(self):
        ''' The set of configured section names, including DEFAULT. '''
        sections = set(['DEFAULT'])
        if self.parser is not None:
            sections |= set(self.parser.sections())
        sections |= set(self._sections.keys())
        return sections


def dict_value_expandvar(d):
    """ Expand all environment variables in the values of given dict.
        Uses os.path.expandvars internally. """

    for key in d:
        try:
            d[key] = os.path.expandvars(d[key])
        except TypeError:
            pass  # d[key] is not a string
    return d


def get_truthy(value):
    """ Returns True on non-zero numbers and on '1', 'true', 'yes', 'on'.
        False otherwise. """
    truthy = ['1', 'true', 'yes', 'on']
    if isinstance(value, Number):
        return bool(value)
    else:
        return value.lower() in truthy


def issequence(obj):
    """ True if given object is a list or a tuple. """
    return isinstance(obj, (list, tuple))


def expandfilename(filename):
    """ Joins sequences of filenames as directories, and expands variables and
        user directory. """
    if issequence(filename):
        filename = os.path.join(*filename)
    return os.path.expandvars(os.path.expanduser(filename))


def expandfilenames(filenames):
    """ Runs expandfilename on each item of a given list. """
    if not issequence(filenames):
        filenames = [filenames]
    return [expandfilename(fname) for fname in filenames]


def write_json(fname, obj):
    """ Write given object to the file referenced by fname. """
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)


def listfiles(mypath):
    """ Returns a list of regular filenames in given directory. """
    return [f
            for f in os.listdir(mypath)
            if os.path.isfile(os.path.join(mypath, f))]


def listdirs(mypath):
    """ Returns a list of directories in given directory. """
    return [d
            for d in os.listdir(mypath)
            if os.path.isdir(os.path.join(mypath, d))]


def copyglob(srcglob, dstdir, prefix=""):
    """ Copy a number of files in glob (wildcard) to a directory.
        Raises ValueError if the destination is not a directory. Each filename
        is first prefixed with given prefix. """
    if not os.path.isdir(dstdir):
        raise ValueError("Destination of copyglob must be a directory")

    for src in glob.glob(expandfilename(srcglob)):
        _, fname = os.path.split(src)
        shutil.copyfile(src, os.path.join(dstdir, prefix + fname))
