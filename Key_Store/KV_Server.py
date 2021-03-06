#!/usr/local/bin/python3
import socket
import json
import logging
import sys
import glob
import os

# sys.path.append('Key_Store')
  
# import thread module 
import threading 

from xmlrpc.server import SimpleXMLRPCServer
  
safe_lock = threading.Lock()

def getDataFromMapReduceFile(data):
    
    raw_data = data.split(' ')
    filename = raw_data[1]
    key = raw_data[2]

    try:
        with open(filename, 'r') as infile:
            ret_data = json.load(infile)

            print(len(ret_data[key]))

            if len(data) != 0:
                return json.dumps(ret_data) + '\n'
                
    except Exception as e:
        logging.exception('Excpetion raised for acquiring data from kv store '+ str(e))

    logging.error('Error fetching data in key store.')
    return 'error_get'

def setDataToMapReduceFile(value):

    jsonData = value.split('\n')[1]
    raw_data = value.split('\n')[0].split(' ')

    filename = raw_data[1]
    key = raw_data[2]
    payload = json.loads(jsonData)

    print(filename, key)

    try:
        with open(filename, 'w+') as file:
            json.dump(payload, file)
            logging.info('Success in dump data at keystore.')
            return True
    except Exception as e:
        logging.exception('Exception raised for dumping data in key value store ' + str(e))

    logging.error('Error fetching data in keystore.')
    return False

def setDataToFile(data):
    jsonData = data.split('\n')[1]
    raw_data = data.split('\n')[0].split(' ')

    filename = raw_data[1]
    try:
        with open(filename, 'w+') as f:
            f.write(jsonData)
            logging.info('Success in dump output data at keystore.')
            return True
    except Exception as e:
        logging.exception('Exception raised for dumping data in key value store ' + str(e))

    logging.error('Error fetching data in keystore.')
    return False

def mapReduceHandler(data):
    print('Inside map reduce handler')
    logging.info('Inside map reduce handler function in Keystore.')
    raw_data = data.split('\n')[0]
    selection = raw_data.split(' ')[0]

    if selection == 'set':
        safe_lock.acquire()
        res = setDataToMapReduceFile(data)
        safe_lock.release()

        if res == True:
            return ('STORED \\r\\n')
        else:
            return ('NOT-STORED \\r\\n')

    if selection == 'get':

        safe_lock.acquire()
        res = getDataFromMapReduceFile(data)
        safe_lock.release()

        print(len(res))

        if res != None:
            return (res)
    
    if selection == 'setOp':

        safe_lock.acquire()
        res = setDataToFile(data)
        safe_lock.release()

        print(len(res))
        if res != None:
            return res

def getStatus():
    logging.info('Checked status of keystore.')
    return 'OK'

def flushFiles():
    files = glob.glob('*.json')
    for f in files:
        os.remove(f)
    return 'OK'

def main():
    try:
        print('Starting a KV Server')
        logging.info('Key store server started. Awaiting connection.')
        
        server = SimpleXMLRPCServer(("", 3389), allow_none=True)
        server.register_introspection_functions()
        server.register_function(mapReduceHandler, 'mapReduceHandler')
        server.register_function(getStatus, 'getStatus')
        server.register_function(flushFiles, 'flushFiles')

        print('Waiting for connection...')
        server.serve_forever()

    except Exception as e:
        logging.exception('Exception at store server: '+ str(e))

if __name__ == "__main__":

    logging.basicConfig(filename='keystore.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    main()
    