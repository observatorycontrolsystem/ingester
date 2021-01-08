# Ingester Library

[![Build Status](https://travis-ci.com/observatorycontrolsystem/ingester.svg?branch=master)](https://travis-ci.com/observatorycontrolsystem/ingester)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/24eb8debeb0c499ca192b4497a1f1e12)](https://www.codacy.com/gh/observatorycontrolsystem/ingester?utm_source=github.com&utm_medium=referral&utm_content=observatorycontrolsystem/ingester&utm_campaign=Badge_Grade)

A library for adding new science data products to an observatory control system's science archive. The library
handles uploading FITS files into AWS S3, as well as adding records to the science archive's database containing
the searchable metadata of all available FITS files.

## Prerequisites

Optional prerequisites may be skipped for reduced functionality.

-   Python >= 3.6
-   A running [science archive](https://github.com/observatorycontrolsystem/science-archive)
-   Write access to the same S3 bucket that the running science archive is using
-   (Optional) A running [OpenTSDB](http://opentsdb.net/) for metrics collection

## Installation

It is highly recommended that you install and run your python code inside a dedicated python
[virtual environment](https://docs.python.org/3/tutorial/venv.html).

Add the `ocs_ingester` package to your python environment:

```bash
(venv) $ pip install ocs_ingester
```

## Configuration

AWS and science archive credentials must be set in order to upload data. Science archive configuration as well as the
AWS Bucket can be either passed explicitly or set as environment variables. The rest of the configuration must be
set as environment variables.

#### Environment Variables

|                 | Variable                            | Description                                                                                                                                                                                                                                | Default                    |
| --------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------- |
| Science Archive | `API_ROOT`                          | Science Archive URL                                                                                                                                                                                                                        | `"http://localhost:8000/"` |
|                 | `AUTH_TOKEN`                        | Science Archive Authentication Token. This token must be associated with an admin user.                                                                                                                                                    | _empty string_             |
| AWS             | `BUCKET`                            | AWS S3 Bucket Name                                                                                                                                                                                                                         | `ingestertest`             |
|                 | `AWS_ACCESS_KEY_ID`                 | AWS Access Key with write access to the S3 bucket                                                                                                                                                                                          | _empty string_             |
|                 | `AWS_SECRET_ACCESS_KEY`             | AWS Secret Access Key                                                                                                                                                                                                                      | _empty string_             |
|                 | `AWS_DEFAULT_REGION`                | AWS S3 Default Region                                                                                                                                                                                                                      | _empty string_             |
| Metrics         | `OPENTSDB_HOSTNAME`                 | OpenTSDB Host to send metrics to                                                                                                                                                                                                           | _empty string_             |
|                 | `OPENTSDB_PYTHON_METRICS_TEST_MODE` | Set to any value to turn off metrics collection                                                                                                                                                                                            | `False`                    |
|                 | `INGESTER_PROCESS_NAME`             | A tag set with the collected metrics to identify where the metrics are coming from                                                                                                                                                         | `ingester`                 |
|                 | `SUBMIT_METRICS_ASYNCHRONOUSLY`     | Optionally submit metrics asynchronously. This option does not apply when the command line entrypoint is used, in which case metrics are always submitted synchronously. Note that some metrics may be lost when submitted asynchronously. | `False`                    |


## For Developers

#### Running the Tests

After cloning this project, from the project root and inside your virtual environment:

```bash
(venv) $ pip install -r requirements.txt
(venv) $ pytest
```
