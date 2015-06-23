from setuptools import setup, find_packages

setup(
    name='prompt',
    packages=find_packages(exclude=['test', 'test*' 'scripts', 'scripts*'], include=['prompt','prompt.*']),
    version=0.1)
