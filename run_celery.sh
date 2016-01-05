#!/bin/sh
celery -A tasks worker -f /dev/null &
celery -A tasks beat
