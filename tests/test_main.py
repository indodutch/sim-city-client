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

import simcity.__main__ as main
from nose.tools import assert_true
import argparse

cmds = ['cancel', 'check', 'create', 'delete', 'init', 'run', 'scrub',
        'summary', 'submit']


class MockArgumentParser(argparse.ArgumentParser):
    def error(self, *args, **kwargs):
        # just return parsed arguments, no matter what
        pass


def test_main_parser_init():
    global cmds

    parser = MockArgumentParser()
    main.fill_argument_parser(parser)

    for cmd in cmds:
        yield assert_true, 'func' in parser.parse_args([cmd])
