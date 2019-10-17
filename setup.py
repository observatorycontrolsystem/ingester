from setuptools import setup

# Read the contents of the README
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ingester',
    version='0.0.9',
    description='Ingest frames into the LCO Archive',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/LCOGT/ingester',
    packages=['ingester', 'ingester.utils', 'settings', 'scripts'],
    python_requires='>=3.5',
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ],
    install_requires=[
        'astropy',
        'requests',
        'boto3',
        'python-dateutil',
        'lcogt-logging',
        'opentsdb-python-metrics>=0.1.8'
    ],
    entry_points={
        'console_scripts': [
            'ingest_frame = scripts.ingest_frame:main',
        ]
    }
)
