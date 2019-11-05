Archive Ingester
================

This application watches a queue for filenames, uploads .fits files to s3,
and posts to the archive API new data products.

Requirements
------------

Rabbitmq

Archive API

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

`run_celery.sh` is a convience script that can be used to launch celery locally for testing.

