# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>
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

from ConfigParser import ConfigParser, NoSectionError
import json
import os
import glob
import shutil


class Config(object):
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

        self.sections = {}

    def add_section(self, name, keyvalue):
        self.sections[name] = keyvalue

    def section(self, name):
        try:
            return self.sections[name]
        except:
            if self.parser is not None:
                return dict(self.parser.items(name))
            else:
                raise NoSectionError(name)


def issequence(obj):
    return isinstance(obj, (list, tuple))


def expandfilename(filename):
    if issequence(filename):
        filename = os.path.join(*filename)
    return os.path.expandvars(os.path.expanduser(filename))


def expandfilenames(filenames):
    if not issequence(filenames):
        filenames = [filenames]
    return [expandfilename(fname) for fname in filenames]


def write_json(fname, obj):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)


def listfiles(mypath):
    return [f
            for f in os.listdir(mypath)
            if os.path.isfile(os.path.join(mypath, f))]


def listdirs(mypath):
    return [d
            for d in os.listdir(mypath)
            if os.path.isdir(os.path.join(mypath, d))]


def copyglob(srcglob, dstdir, prefix=""):
    if not os.path.isdir(dstdir):
        raise ValueError("Destination of copyglob must be a directory")

    for src in glob.glob(expandfilename(srcglob)):
        _, fname = os.path.split(src)
        shutil.copyfile(src, os.path.join(dstdir, prefix + fname))
