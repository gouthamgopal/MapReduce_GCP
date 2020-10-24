#!/bin/bash
sudo apt-get update
sudo apt-get -y install python3-venv
cd /home/ggopal
git clone -b master https://github.com/gouthamgopal/MapReduce_GCP.git
cd MapReduce_GCP
cd Master
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
# pip3 install googleapiclient
echo 'Master logger file' > master.log
chmod u+x master.log
python3 master.py