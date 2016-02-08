#!/bin/sh
celery -A tasks worker -Q celery -n main_worker &
celery -A tasks worker -Q periodic --concurrency=1 -n periodic_worker &
celery -A tasks beat &
celery -A tasks flower
