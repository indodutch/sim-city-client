from __future__ import print_function

import unittest
from simcity.util import Config, expandfilenames, merge_dicts, issequence, expandfilename, Timer
import os, tempfile
import ConfigParser
import time

class TestConfig(unittest.TestCase):
    def testWriteRead(self):
        fd, fname = tempfile.mkstemp()
        f = os.fdopen(fd,'w')
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
        self.assertRaises(ConfigParser.NoSectionError, cfg.section, 'NonExistantSection')
        self.assertIs(type(my), dict, "section is not a dictionary")
        self.assertTrue('a' in my, "Value in section")
        self.assertEqual(my['a'], '4', "latest value does not overwrite earlier values")
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
        value = expandfilenames(['config.ini', ['~', 'home'], ('..', 'config.ini')])
        expected = ['config.ini', os.path.expanduser('~/home'), '../config.ini']
        self.assertEqual(value, expected)
        
        self.assertEqual(expandfilenames('config.ini'), ['config.ini'])
        self.assertEqual(expandfilenames([]), [])

    def testPath(self):
        self.assertEqual(expandfilename('config.ini'), 'config.ini')
        self.assertEqual(expandfilename(['~', 'home']), os.path.expanduser('~/home'))

class TestMerge(unittest.TestCase):
    def setUp(self):
        self.a = {'a': 1, 'b': 2}
        self.b = {'a': 2, 'c': 3}
        
    def testMergeAll(self):
        c = merge_dicts(self.a, self.b)
        self.assertEqual(c['a'], self.b['a'])
        self.assertEqual(self.b['a'], 2)
        self.assertEqual(self.a['a'], 1)
        self.assertEqual(len(self.a), 2)
        self.assertEqual(len(self.b), 2)
        self.assertEqual(len(c), 3)
        self.assertEqual(c['b'], self.a['b'])
        self.assertEqual(c['c'], self.b['c'])
    
    def testMergeEmpty(self):
        c = merge_dicts(self.a, {})
        self.assertDictEqual(c, self.a)

    def testEmptyMerge(self):
        c = merge_dicts({}, self.a)
        self.assertDictEqual(c, self.a)
    
    def testEmptyEmptyMerge(self):
        self.assertDictEqual(merge_dicts({}, {}), {})
        
class TestTimer(unittest.TestCase):
    def testTimer(self):
        timer = Timer()
        time.sleep(0.2)
        self.assertGreaterEqual(timer.elapsed, 0.2)
        self.assertLess(timer.elapsed, 0.4)
        timer.reset()
        self.assertLess(timer.elapsed, 0.2)
