from setuptools import setup

# TODO: Add license info

# Read the contents of the README
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ingester',
    version='0.0.5',
    description='Ingest frames into the LCO Archive',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/LCOGT/ingester',
    packages=['ingester', 'settings', 'scripts'],
    python_requires='>=3.5',
    install_requires=[
        'astropy>=3.2,<3.3',
        'requests>=2.0,<3.0',
        'boto3>=1.7,<1.8',
        'python-dateutil>=2.7,<2.8',
        'lcogt-logging',
        'opentsdb-python-metrics>=0.1.8'
    ],
    entry_points={
        'console_scripts': [
            'frame_exists = scripts.frame_exists:main',
        ]
    }
)
