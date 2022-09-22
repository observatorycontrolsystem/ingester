# Ingester Library

![Build](https://github.com/observatorycontrolsystem/ingester/workflows/Build/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/observatorycontrolsystem/ingester/badge.svg?branch=master)](https://coveralls.io/github/observatorycontrolsystem/ingester?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/24eb8debeb0c499ca192b4497a1f1e12)](https://www.codacy.com/gh/observatorycontrolsystem/ingester?utm_source=github.com&utm_medium=referral&utm_content=observatorycontrolsystem/ingester&utm_campaign=Badge_Grade)

A library for adding new science data products to an observatory control system's [Science Archive](https://github.com/observatorycontrolsystem/science-archive/).
The library handles uploading files into a `FileStore` (S3 recommended), as well as adding records to the Science Archive's database containing
the searchable metadata of all available files. Optionally, it records ingestion metrics in an [openTSDB](http://opentsdb.net/) instance, which can be disabled by setting the environment variable `OPENTSDB_PYTHON_METRICS_TEST_MODE=False`.

## Prerequisites

Optional prerequisites may be skipped for reduced functionality.

-   Python >= 3.7
-   A running [Science Archive](https://github.com/observatorycontrolsystem/science-archive/)
-   A configured FileStore (S3 recommended) with write access to store data there
-   (Optional) A running [OpenTSDB](http://opentsdb.net/) for metrics collection

## Usage
This project depends on the OCS [Archive Library](https://github.com/observatorycontrolsystem/ocs_archive/), so please look through and set all of its environment variables to match your file format header keys and data storage choices. Specifically, the `FILESTORE_TYPE` environment variable must be set for your FileStore backend, since it defaults to `dummy` which stores no data. The header mapping environment variables should also be set to map to the correct keys in your data products as well. These environment variable values must match the values used in your [Science Archive](https://github.com/observatorycontrolsystem/science-archive/) instance as well. Also check out the [data flow documentation](https://observatorycontrolsystem.github.io/integration/data_flow/) for more details on how to configure and use the ingester.

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
|                 | `S3_ENDPOINT_URL`                   | Endpoint url for connecting to s3. This can be modified to connect to a local instance of s3.                                                                                                                                              | `"http://s3.us-west-2.amazonaws.com"` |
| Metrics         | `OPENTSDB_HOSTNAME`                 | OpenTSDB Host to send metrics to                                                                                                                                                                                                           | _empty string_             |
|                 | `OPENTSDB_PYTHON_METRICS_TEST_MODE` | Set to any value to turn off metrics collection                                                                                                                                                                                            | `False`                    |
|                 | `INGESTER_PROCESS_NAME`             | A tag set with the collected metrics to identify where the metrics are coming from                                                                                                                                                         | `ingester`                 |
|                 | `SUBMIT_METRICS_ASYNCHRONOUSLY`     | Optionally submit metrics asynchronously. This option does not apply when the command line entrypoint is used, in which case metrics are always submitted synchronously. Note that some metrics may be lost when submitted asynchronously. | `False`                    |


## For Developers

#### Running the Tests

After cloning this project, from the project root and inside your virtual environment:

```bash
(venv) $ pip install -e .[tests]
(venv) $ pytest
```
