#!/usr/bin/env python

#!/usr/bin/env python

from __future__ import print_function
import sys
import json
import os
import subprocess
from simcity_client.util import expandfilename, copyglob

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

    for dstdir in dirs.values():
        os.mkdir(dstdir)
    
    # Set up runtime directories
    copyglob((src, 'control', '*.ctl'), dirs['control'])
    copyglob((src, 'input', '*'), dirs['input'])
    
    # Run simulation
    os.chdir(dirs['control'])
    subprocess.call(['./RunAll.sh'])
    
    # Copy output files to output directory
    copyglob((dirs['control'], '*.prn'), out_dir)
    copyglob((dirs['ArcGis'], '*'), out_dir)
    copyglob((dirs['output'], '*'), out_dir)
    
    print("Done")
