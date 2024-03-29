import os
from setuptools import setup, find_packages

requirements = []
requirements_file = 'requirements.txt'

# Check if the file exists
if os.path.exists(requirements_file):
    with open(requirements_file) as f:
        requirements = f.read().splitlines()

setup(
    name='GoogleCloudPlatformAPI',
    version='v2.2.3',
    packages=find_packages(),
    install_requires=requirements,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)
