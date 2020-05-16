Ingester Library
================
[![Build Status](https://travis-ci.com/observatorycontrolsystem/ingester.svg?branch=master)](https://travis-ci.com/observatorycontrolsystem/ingester)

A library for adding new science data products to an observatory control system's science archive. The library
handles uploading FITS files into AWS S3, as well as adding records to the science archive's database containing
the searchable metadata of all available FITS files.

## Prerequisites
Optional prerequisites may be skipped for reduced functionality.

- Python >= 3.6
- A running [science archive](https://github.com/observatorycontrolsystem/science-archive)
- Write access to the same S3 bucket that the running science archive is using
- (Optional) A running [OpenTSDB](http://opentsdb.net/) for metrics collection

## Installation

It is highly recommended that you install and run your python code inside a dedicated python
[virtual environment](https://docs.python.org/3/tutorial/venv.html).

Add the `lco_ingester` package to your python environment:

```bash
(venv) $ pip install lco_ingester
```

## Configuration

AWS and science archive credentials must be set in order to upload data. Science archive configuration as well as the
AWS Bucket can be either passed explicitly or set as environment variables. The rest of the configuration must be
set as environment variables.

#### Environment Variables

|  | Variable | Description | Default |
| --- | -------- | ----------- | ------- |
| Science Archive | `API_ROOT` | Science Archive URL | `"http://localhost:8000/"` |
| | `AUTH_TOKEN` | Science Archive Authentication Token. This token must be associated with an admin user. | *empty string* |
| AWS | `BUCKET` | AWS S3 Bucket Name | `ingestertest` |
| | `AWS_ACCESS_KEY_ID` | AWS Access Key with write access to the S3 bucket | *empty string* |
| | `AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key | *empty string* |
| | `AWS_DEFAULT_REGION` | AWS S3 Default Region | *empty string* |
| Metrics | `OPENTSDB_HOSTNAME` | OpenTSDB Host to send metrics to | *empty string* |
| | `OPENTSDB_PYTHON_METRICS_TEST_MODE` | Set to any value to turn off metrics collection | `False` |
| | `INGESTER_PROCESS_NAME` | A tag set with the collected metrics to identify where the metrics are coming from | `ingester` |
| | `SUBMIT_METRICS_ASYNCHRONOUSLY` | Optionally submit metrics asynchronously. This option does not apply when the command line entrypoint is used, in which case metrics are always submitted synchronously. Note that some metrics may be lost when submitted asynchronously. | `False` |
| Postprocessing  | `FITS_BROKER` | FITS exchange broker  | `memory://localhost` |
| | `PROCESSED_EXCHANGE_NAME` | Processed files RabbitMQ Exchange Name | `archived_fits` |
| | `POSTPROCESS_FILES` | Optionally submit files to fits queue  | `True` |

## Examples
Triple arrows (>>>) are used to show the output of a function.

#### Ingest a file step-by-step

```python
from lco_ingester import ingester

with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:

    ingester.frame_exists(fileobj)
    >>> False

    record = ingester.validate_fits_and_create_archive_record(fileobj)
    >>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'FILTER': 'rp', 'DATE-OBS': '2019-10-13T10:13:00', ... }

    s3_version = ingester.upload_file_to_s3(fileobj)
    >>> {'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}

    ingested_record = ingester.ingest_archive_record(s3_version, record)
    >>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'version_set': [{'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}], 'frameid': 400321, ... }
```

#### Ingest a file in one step

```python
from lco_ingester import ingester

with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    ingester.upload_file_and_ingest_to_archive(fileobj)
    >>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'version_set': [{'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}], 'frameid': 400321, ... }
```

#### Using the command line entry point
A command line script for ingesting data, and optionally only checking if that data already exists
in the science archive, is available for use as well.

```bash
(venv) lco_ingest_frame --help  # See available options
```

## For Developers

#### Running the Tests
After cloning this project, from the project root and inside your virtual environment:
```bash
(venv) $ pip install -r requirements.txt
(venv) $ pytest
````
