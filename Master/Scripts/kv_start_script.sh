#!/bin/bash
sudo apt-get update

cd /home/ggopal
git clone -b key_store https://github.com/gouthamgopal/MapReduce_GCP.git
cd MapReduce_GCP
cd Key_Store
echo 'Key Store logger file' > keystore.log
chmod u+x keystore.log
python3 KV_Server.py