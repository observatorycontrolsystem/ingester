from setuptools import setup

# Read the contents of the README
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='lco-ingester',
    version='2.1.12',
    description='Ingest frames into the LCO Archive',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/LCOGT/ingester',
    packages=['lco_ingester', 'lco_ingester.utils', 'lco_ingester.settings', 'lco_ingester.scripts'],
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
        'kombu',
        'opentsdb-python-metrics>=0.1.8'
    ],
    tests_require=[
        'pytest',
        # celery is not required by the library, but there are tests that test the ingester application
        # which require it.
        'celery>=4.1,<4.2',
    ],
    entry_points={
        'console_scripts': [
            'lco_ingest_frame = lco_ingester.scripts.ingest_frame:main',
        ]
    }
)
