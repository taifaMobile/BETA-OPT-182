#!/bin/bash

cd /opt
nohup celery -A tasks flower --loglevel=info &
screen -S workersLauncher -d -m /opt/start_workers.sh


