# Archive Ingester

Upload .fits files to S3 and post new data products to the Archive API.

## Installation
Add the `ingester` package to your python environment:

`(venv) $ pip install ingester`

## Configuration

AWS and Archive API credentials must be set in order to upload data. Archive API configuration as well as the
AWS Bucket can be either passed in as kwargs or set as environment variables. The rest of the configuration must be
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



## Ingester Library API
<!-- TODO: convert this to use pydoc and the function docstrings -->

    frame_exists(path, **kwargs)

    Checks if the frame exists in the archive.

---
    validate_fits_and_create_archive_record(path, **kwargs)

    Validate the fits file and also create an archive record from it.

---
    upload_file_to_s3(path, **kwargs)

    Upload a file to S3.

---
    ingest_archive_record(version, record, **kwargs)

    Ingest an archive record.

---
    upload_file_and_ingest_to_archive(path, **kwargs)

    Ingest and upload a file.

---

#### Exceptions

Exceptions raised by the ingester code are described in the `ingester.exceptions` module.

## Examples

#### Ingest a file step-by-step

```python
from ingester import ingester

ingester.frame_exists('tst1mXXX-ab12-20191013-0001-e00.fits.fz')
>>> False

record = ingester.validate_fits_and_create_archive_record('tst1mXXX-ab12-20191013-0001-e00.fits.fz')
>>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'FILTER': 'rp', 'DATE-OBS': '2019-10-13T10:13:00', ... }

s3_version = ingester.upload_file_to_s3('tst1mXXX-ab12-20191013-0001-e00.fits.fz')
>>> {'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}

ingested_record = ingester.ingest_archive_record(s3_version, record)
>>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'version_set': [{'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}], 'frameid': 400321, ... }
```

#### Ingest a file, do all steps at once!

```python
from ingester import ingester

ingester.upload_file_and_ingest_to_archive('tst1mXXX-ab12-20191013-0001-e00.fits.fz')
>>> {'basename': 'tst1mXXX-ab12-20191013-0001-e00', 'version_set': [{'key': '792FE6EFFE6FAD7E', 'md5': 'ECD9B357D67117BE8BF38D6F4B4A6', 'extension': '.fits.fz'}], 'frameid': 400321, ... }
```

#### Using the command line entry point
A command line script for ingesting data, and optionally only checking if that data already exists
in the Archive API, is available for use as well.

```commandline
ingest_frame --help  # See available options
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
You will need a RabbitMQ server running with the environment variable `FITS_BROKER` pointing to it. The other
environment variables in the Configuration section should be set as well.

#### Running
`listener.py` Will listen on the configured queue for new messages. When one is received,
it will launch an asynchronous celery task to ingest the file.

`run_celery.sh` is a convenience script that can be used to launch celery locally for testing.

A `Dockerfile` is available that can be used to run the application.
