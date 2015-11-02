#!/usr/bin/env python
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

from __future__ import print_function
import sys
import json
import os

if __name__ == '__main__':
    print("Input:")
    print(sys.argv)

    tmp_dir = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]

    with open(os.path.join(in_dir, 'input.json'), 'r') as f:
        params = json.load(f)

    output = float(params['a']) * 51.0
    print("Output:")
    print(output)

    with open(os.path.join(out_dir, 'a_out'), 'w') as f:
        print(output, file=f)

    print("Done")
