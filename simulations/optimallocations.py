#!/usr/bin/env python
""" Very basic optimization problem with multiple local minima. """

from __future__ import print_function
import json
import os
import numpy as np


def response_time(x, y):
    if x < 0 or x > 1 or y < 0 or y > 1:
        return 0

    # uniform random sample [0,1]^2
    fire_stations = np.array([
        [0.88589305, 0.56027193],
        [0.04232427, 0.35768432],
        [0.97446965, 0.78589876],
        [0.98890171, 0.58330911],
        [0.91564589, 0.67090423],
        [0.75081924, 0.03186881],
        [0.91467317, 0.94835546],
        [0.31517691, 0.81988699],
        [0.72430129, 0.34017654],
        [0.00629674, 0.41492913],
        [0.3720539,  0.83472615],
        [0.88162608, 0.15670971],
    ])
    all_dist = np.linalg.norm(fire_stations - np.array([x, y]), axis=1)
    return np.min(all_dist)


if __name__ == '__main__':
    with open(os.environ['SIMCITY_PARAMS'], 'r') as f:
        params = json.load(f)

    t = response_time(params['x'], params['y'])

    print('f({0}) = {1}'.format(params, t))

    result_filename = os.path.join(
        os.environ['SIMCITY_OUT'], 'response_time.csv')

    with open(result_filename, 'w') as f:
        f.write(str(t))
