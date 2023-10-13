from setuptools import setup, find_packages

setup(
    name='GoogleCloudPlatformAPI',
    version='1.11',
    packages=find_packages(),
    install_requires=[
        "pandas",
        "google-api-python-client",
        "google-cloud-bigquery",
        "google-cloud-storage"
    ],
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)
