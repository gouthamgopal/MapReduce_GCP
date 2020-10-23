#!/bin/bash
cd /home/ggopal
echo 'In home/gopal' > log.txt
git clone -b key_store https://github.com/gouthamgopal/MapReduce_GCP.git
echo 'Cloned from git' > log.txt
cd MapReduce_GCP
cd Key_Store
echo 'Key Store logger file' > keystore.log
chmod u+x keystore.log
python3 KV_Server.py