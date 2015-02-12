#!/usr/bin/env python

from __future__ import print_function
import sys
import json
import os
import subprocess
from simcity.util import expandfilename, copyglob, listdirs

if __name__ == '__main__':
    print("Input:")
    print(sys.argv)
    
    tmp_dir = sys.argv[1]
    in_dir = sys.argv[2]
    out_dir = sys.argv[3]
    
    with open(os.path.join(in_dir, 'input.json'), 'r') as f:
        params = json.load(f)
    
    print("Parameters:")
    print(params)
    
    try:
        model = params['model']
    except:
        model = 'tutorial'
    
    src = expandfilename(['~', 'bangalore-matsim'])
    
    indir = expandfilename([tmp_dir, 'input'])
    moddir = expandfilename([indir, model])
    outdir = expandfilename([tmp_dir, 'output', model])
    
    os.mkdir(indir)
    os.mkdir(moddir)
    # don't make output directory
    
    # Set up runtime directories
    copyglob([src, 'input', model, '*'], moddir)
    
    # Run simulation
    os.chdir(tmp_dir)
    subprocess.call([expandfilename([src, 'scripts', 'matsim-runner.sh']), model])
    
    # Copy output files to output directory
    copyglob(expandfilename([outdir, '*.(log|gz|png|txt)']), out_dir)
    
    itersdir = expandfilename([outdir, 'ITERS'])
    for d in listdirs(itersdir):
        iterglob = expandfilename([itersdir, d, '*'])
        copyglob(iterglob, out_dir, prefix="ITER_" + d + "_")
    
    print("Done")
