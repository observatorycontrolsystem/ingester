#!/bin/sh
celery -A tasks worker --loglevel=WARNING &
celery -A tasks beat
