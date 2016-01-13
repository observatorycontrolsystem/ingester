#!/bin/sh
celery -A tasks worker &
celery -A tasks beat &
celery -A tasks flower
