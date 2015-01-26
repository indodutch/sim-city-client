#!/usr/bin/env python

from distutils.core import setup

setup(name='simcity_client',
      version='0.1',
      description='Python SIM-CITY client using PiCaS.',
      author='Joris Borgdorff',
      author_email='j.borgdorff@esciencecenter.nl',
      url='https://esciencecenter.nl/projects/sim-city/',
      packages=['simcity_client'],
      install_requires=['picas', 'couchdb', 'pystache']
     )
