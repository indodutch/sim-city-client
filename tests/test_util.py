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

from __future__ import print_function

import unittest
from simcity.util import (Config, expandfilenames, issequence,
                          expandfilename)
import os
import tempfile
import ConfigParser


class TestConfig(unittest.TestCase):

    def testWriteRead(self):
        fd, fname = tempfile.mkstemp()
        f = os.fdopen(fd, 'w')
        print('[MySection]', file=f)
        print('a=1', file=f)
        print('a=4', file=f)
        print('[OtherSection]', file=f)
        print('a=2', file=f)
        print('b=wefa feaf', file=f)
        print('c=wefa=feaf', file=f)
        f.close()

        cfg = Config(fname)
        os.remove(fname)

        my = cfg.section('MySection')
        other = cfg.section('OtherSection')
        self.assertRaises(
            ConfigParser.NoSectionError, cfg.section, 'NonExistantSection')
        self.assertTrue(type(my) is dict, "section is not a dictionary")
        self.assertTrue('a' in my, "Value in section")
        self.assertEqual(
            my['a'], '4', "latest value does not overwrite earlier values")
        self.assertEqual(other['a'], '2', "value not contained to section")
        self.assertEqual(other['b'], 'wefa feaf', "spaces allowed")
        self.assertEqual(other['c'], 'wefa=feaf', "equals-sign allowed")


class TestExistingPath(unittest.TestCase):

    def testSeq(self):
        self.assertTrue(issequence(()))
        self.assertTrue(issequence(("a", "b")))
        self.assertTrue(issequence([]))
        self.assertTrue(issequence(["a"]))
        self.assertFalse(issequence(""))
        self.assertFalse(issequence("a"))
        self.assertFalse(issequence(1))
        self.assertFalse(issequence({}))
        self.assertFalse(issequence(set()))

    def testPaths(self):
        value = expandfilenames(
            ['config.ini', ['~', 'home'], ('..', 'config.ini')])
        expected = [
            'config.ini', os.path.expanduser('~/home'), '../config.ini']
        self.assertEqual(value, expected)

        self.assertEqual(expandfilenames('config.ini'), ['config.ini'])
        self.assertEqual(expandfilenames([]), [])

    def testPath(self):
        self.assertEqual(expandfilename('config.ini'), 'config.ini')
        self.assertEqual(
            expandfilename(['~', 'home']), os.path.expanduser('~/home'))
