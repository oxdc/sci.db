from setuptools import setup

setup(
   name='oxdc-scidb',
   version='0.1b3',
   description='A simple scientific database.',
   author='oxdc',
   author_email='projaias@outlook.com',
   url='https://github.com/oxdc/sci.db',
   packages=[
      'scidb',
      'scidb.core', 'scidb.core.low',
      'scidb.client', 'scidb.client.modules',
      'scidb.utils',
      'scidb.plugins',
      'scidb.plugins.backup', 'scidb.plugins.backup.base', 'scidb.plugins.backup.implementations',
      'scidb.plugins.sandbox'
   ],
   install_requires=['PyYAML', 'minio', 'urllib3']
)
