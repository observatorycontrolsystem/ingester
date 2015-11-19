#!/bin/sh
celery -A tasks worker --loglevel=WARNING
