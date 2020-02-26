# Archive Ingester
[![Ingester build Status](https://api.travis-ci.com/LCOGT/ingester.svg?branch=master)](https://travis-ci.org/LCOGT/ingester)

Upload .fits files to S3 and post new data products to the Archive API.

## Installation
Add the `lco_ingester` package to your python environment:

`(venv) $ pip install lco_ingester`

## Configuration

AWS and Archive API credentials must be set in order to upload data. Archive API configuration as well as the
AWS Bucket can be either passed explicitly or set as environment variables. The rest of the configuration must be
set as environment variables.

#### Environment Variables
| | Variable | Description | Default
| --- | --- | --- | ---
| Archive API | `API_ROOT` | Archive API URL | `"http://localhost:8000/"`
| | `AUTH_TOKEN` | Archive API Authentication Token | `""`
| AWS | `BUCKET` | AWS S3 Bucket Name | `ingestertest`
| | `AWS_ACCESS_KEY_ID` | AWS Access Key | `""`
| | `AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key | `""`
| | `AWS_DEFAULT_REGION` | AWS S3 Default Region | `""`
| Metrics | `OPENTSDB_HOSTNAME` | OpenTSDB Host to send metrics to | `""`
| | `OPENTSDB_PYTHON_METRICS_TEST_MODE` | Set to any value to turn off metrics collection | `False`
| | `INGESTER_PROCESS_NAME` | A tag set with the collected metrics to identify where the metrics are coming from | `ingester`
| | `SUBMIT_METRICS_ASYNCHRONOUSLY` | Optionally submit metrics asynchronously. This option does not apply when the command line entrypoint is used, in which case metrics are always submitted synchronously. Note that some metrics may be lost when submitted asynchronously. | `False`
| Postprocessing | `FITS_BROKER` | FITS exchange broker  | `memory://localhost`
| | `PROCESSED_EXCHANGE_NAME` | Processed files RabbitMQ Exchange Name | `archived_fits`
| | `POSTPROCESS_FILES` | Optionally submit files to fits queue  | `True`



## Ingester Library API
<!-- TODO: convert this to use pydoc and the function docstrings -->

    frame_exists(fileobj, [api_root, auth_token])

    Checks if the frame exists in the archive.

---
    validate_fits_and_create_archive_record(fileobj, [path, required_headers, blacklist_headers])

    Validate the fits file and also create an archive record from it.

---
    upload_file_to_s3(fileobj, [path, bucket])

    Upload a file to S3.

---
    ingest_archive_record(version, record, [api_root, auth_token])

    Ingest an archive record.

---
    upload_file_and_ingest_to_archive(fileobj, [path, required_headers, blacklist_headers, api_root, auth_token, bucket])

    Ingest and upload a file.

---

#### Exceptions

Exceptions raised by the ingester code are described in the `lco_ingester.exceptions` module.

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

#### Ingest a file, do all steps at once!

```python
from lco_ingester import ingester

with open('tst1mXXX-ab12-20191013-0001-e00.fits.fz', 'rb') as fileobj:
    ingester.upload_file_and_ingest_to_archive(fileobj)
    >>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'version_set': [{'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}], 'frameid': 400321, ... }
```

#### Using the command line entry point
A command line script for ingesting data, and optionally only checking if that data already exists
in the Archive API, is available for use as well.

```commandline
lco_ingest_frame --help  # See available options
```

## For Developers

#### Running the Tests
The first thing you'll probably want to do after you clone the repo is run the tests:
```
$ cd ingester # the repo you just cloned
$ /path/to/python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
(venv) $ pytest
````

## Ingester Application
In addition to the library, the code provides an application that watches a queue for filenames and ingests
files as they appear.

#### Setup
You will need a RabbitMQ server running with the environment variable `BROKER_URL` pointing to it. You will also
need to set the `FITS_BROKER` environment variable to the RabbitMQ to which the app is watching for new filenames. 
The other environment variables in the Configuration section should be set as well.

#### Running
`listener.py` Will listen on the configured queue for new messages. When one is received,
it will launch an asynchronous celery task to ingest the file.

`run_celery.sh` is a convenience script that can be used to launch celery locally for testing.

A `Dockerfile` is available that can be used to run the application.
