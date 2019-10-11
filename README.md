Archive Ingester
================

This application watches a queue for filenames, uploads .fits files to s3,
and posts to the archive API new data products.

Requirements
------------

Rabbitmq

Archive API

For Library Clients
===================

Ingester Library API
--------------------
<!-- TODO: convert this to use pydoc and the function docstrings --> 

    frame_exists(path, **kwargs)
    
    Checks if the frame exists in the archive.

---
    validate_fits_and_create_archive_record(path, **kwargs)
    
    Validate the fits file and also create an archive record from it.
    After this step the version would still be missing
    Returns the constructed record

---
    upload_file_to_s3(path, **kwargs)
    
    Uploads a file to s3.

---
    ingest_archive_record(version, record, **kwargs)
    
    Ingest an archive record.

---
    upload_file_and_ingest_to_archive(path, **kwargs)
     
    Ingest and upload a file.
    Includes safety checks and the option to record metrics for various steps.

---
    class Ingester(object):
        def __init__(self, path, s3, archive, required_headers=None, blacklist_headers=None)

    Ingest a single file into the archive.
    A single instance of this class is responsible for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    
    For example,
    
    ingester = Ingester(...)
    fits_dict = ingester.ingest()

---


For Developers
==============

Running the Tests
-------------
The first thing you'll probably want to do after you clone the repo is run the tests:

```
$ cd ingester # the repo you just cloned
$ /path/to/python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
(venv) $ tox
````

Setup
-----

You will need a rabbitmq server running. The environmental variable `BROKER_URL`
should point to it. There are a few configuration options, see `settings.py`

You will also need Amazon S3 credentials. The following environmental variables
should be set:

    AWS_ACCESS_KEY
    AWS_SCRET_ACCESS_KEY
    AWS_DEFAULT_REGION
    BUCKET


Running
-------

`listener.py` Will listen on the configured queue for new messages. When once is recieved,
it will launch an asynchronous celery task to ingest the file.

`runcrawler.sh` is a convience script that can be used to launch celery locally for testing.

