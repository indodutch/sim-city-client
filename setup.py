#!/usr/bin/env python

from distutils.core import setup

setup(name='simcity',
      version='0.1',
      description='Python SIM-CITY client using CouchDB as a task pool server.',
      author='Joris Borgdorff',
      author_email='j.borgdorff@esciencecenter.nl',
      url='https://esciencecenter.nl/projects/sim-city/',
      packages=['simcity', 'simcity.task', 'simcity.job'],
      install_requires=['couchdb', 'pystache', 'numpy']
     )
