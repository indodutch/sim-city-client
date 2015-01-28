#!/usr/bin/env python

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
    
    output = float(params['a'])*51.0
    print("Output:")
    print(output)
    
    with open(os.path.join(out_dir, 'a_out'), 'w') as f:
        print(output, file=f)
        
    print("Done")
