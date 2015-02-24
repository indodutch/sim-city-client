#!/usr/bin/env python

from distutils.core import setup
setup(name='simcity',
      version='0.2.1',
      description='Python SIM-CITY client using CouchDB as a task pool server.',
      author='Joris Borgdorff',
      author_email='j.borgdorff@esciencecenter.nl',
      url='https://esciencecenter.nl/projects/sim-city/',
      packages=['simcity', 'simcity.task', 'simcity.job'],
      classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Environment :: Console',
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Topic :: System :: Distributed Computing'
      ],
     )
