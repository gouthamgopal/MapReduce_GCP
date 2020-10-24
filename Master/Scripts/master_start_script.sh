#!/bin/bash
cd /home/ggopal
git clone -b master https://github.com/gouthamgopal/MapReduce_GCP.git
cd MapReduce_GCP
cd Master
echo 'Master logger file' > master.log
chmod u+x master.log
python3 master.py