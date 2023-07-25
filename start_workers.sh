#!/bin/bash

cd /opt
celery -A tasks worker --loglevel=INFO -P eventlet -c 500
