#!/usr/bin/env python
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
import sys
import json
import os
import subprocess
from simcity.util import expandfilename, copyglob

if __name__ == '__main__':
    print("Input:")
    print(sys.argv)
    
    tmp_dir = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]
    
    with open(os.path.join(in_dir, 'input.json'), 'r') as f:
        params = json.load(f)
    
    src = expandfilename(('~', 'transims-us101'))
    dst = expandfilename((tmp_dir, 'transims-us101'))
    
    dirnames = ['control', 'input', 'output', 'ArcGis']
    dirs = {name: expandfilename([dst, name]) for name in dirnames}

    os.mkdir(dst)
    for dstdir in dirs.values():
        os.mkdir(dstdir)
    
    # Set up runtime directories
    copyglob((src, 'control', '*.ctl'), dirs['control'])
    copyglob((src, 'control', '*.sh'), dirs['control'])
    copyglob((src, 'input', '*'), dirs['input'])
    
    # Run simulation
    os.chdir(dirs['control'])
    subprocess.call(['./RunAll.sh'])
    
    # Copy output files to output directory
    copyglob((dirs['control'], '*.prn'), out_dir)
    copyglob((dirs['ArcGis'], '*'), out_dir)
    copyglob((dirs['output'], '*'), out_dir)
    
    print("Done")
