#!/bin/sh
celery -A tasks worker -B --loglevel=WARNING
