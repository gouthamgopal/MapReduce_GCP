#!/bin/bash
cd /home/ggopal
git clone -b worker https://github.com/gouthamgopal/MapReduce_GCP.git
cd MapReduce_GCP
cd Worker
echo 'Worker logger file' > worker.log
chmod u+x worker.log
python3 worker.py