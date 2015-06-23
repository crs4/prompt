from setuptools import setup, find_packages

setup(
    name='prompt',
    packages=find_packages(exclude=['test', 'test*' 'scripts', 'scripts*'], include=['prompt','prompt.*']),
    package_data = {'prompt': ['mining/process/eventlog/serializers/avro_serializer/schemas/*.avsc']},
    version=0.1)
