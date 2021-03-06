#!/usr/local/bin/python3
from multiprocessing import Pool, Process
import string
import sys
import os
import xmlrpc.client
import sys
import glob
import json
import pickle
import logging
import gcp
import time
from configparser import ConfigParser

from xmlrpc.server import SimpleXMLRPCServer

config = {}

def worker_func(map_func, filename, index, mapper_ip, mapper_port):
    # global config
    # print(config)
    print('Inside worker function calling method')
    logging.info('Inside worker function calling method')
    # mapper = config["worker_client"][str(index)]
    if map_func == 'wordcount':
        with xmlrpc.client.ServerProxy("http://{0}:{1}/".format(mapper_ip, str(mapper_port))) as mapper:
            mapper.map_wordcount(filename, index)
    elif map_func == 'invertedindex':
        with xmlrpc.client.ServerProxy("http://{0}:{1}/".format(mapper_ip, str(mapper_port))) as mapper:
            mapper.map_invertedindex(filename, index)
        return

def mapperWorker(map_func, index, file_list, mapper_ip, mapper_port):
    print('Inside process creation for mapper method')
    logging.info('Inside process creation for mapper method')
    mapper = xmlrpc.client.ServerProxy("http://{0}:{1}/".format(mapper_ip, str(mapper_port)))
    print("Inside mapper worker")
    print(map_func, index, mapper_ip, mapper_port)
    print(file_list)
    for file_name in file_list[str(index)]:
        # while True:
            # try:
            #     status = 'RUNNING'
            #     mapper = xmlrpc.client.ServerProxy("http://{0}:{1}/".format(mapper_ip, str(mapper_port)))
            #     print('filename worker: '+file_name)
            #     logging.info('filename worker: '+file_name)
            #     p = Process(target=mapper.worker, args=('map', map_func, file_name, index, ))
            #     p.start()
            #     p.join()
            #     # mapper.worker('map', map_func, file_name, index)

            #     while True:
            #         if mapper.checkWorkStatus() == 'DONE':
            #             status = 'DONE'
            #             logging.info('Worker {0} returned with status DONE'.format(str(index)))
            #             break
            #         else:
            #             time.sleep(5)

            #     if status == 'DONE':
            #         break

            # except Exception as e:
            #     logging.exception(str(e))
            #     logging.info('Restarting the mapper process.')
            #     # mapper.setWorkStatus('IDLE')
            #     continue
        try:
            mapper = xmlrpc.client.ServerProxy("http://{0}:{1}/".format(mapper_ip, str(mapper_port)))
            mapper.worker('map', map_func, file_name, index)

            while True:
                print("waiting for mapper")
                if mapper.checkWorkStatus() == "DONE":
                    break

        except Exception as e:
            logging.exception(str(e))

def reducerWorker(red_func, index, file_list, reducer_ip, reducer_port):
    print('Inside proces creation for reducer method')
    logging.info('Inside proces creation for reducer method')
    # reducer = config["worker_client"][str(index)]
    # if red_func == 'wordcount':
    #     print('reducer_file_name: '+ file_list[index])
    #     with xmlrpc.client.ServerProxy("http://{0}:{1}/".format(reducer_ip, str(reducer_port))) as reducer:
    #         reducer.reducer_helper(red_func, file_list[index], index)
    # elif red_func == 'invertedindex':
    #     print('reducer_file_name: '+ file_list[index])
    #     with xmlrpc.client.ServerProxy("http://{0}:{1}/".format(reducer_ip, str(reducer_port))) as reducer:
    #         reducer.reducer_helper(file_list[index], red_func, index)
    with xmlrpc.client.ServerProxy("http://{0}:{1}".format(reducer_ip, reducer_port)) as reducer:
        reducer.worker('reduce', red_func, file_list[index], index)
        

class MasterServer:
    def __init__(self):
        #initialize all the gcp related things, zones, project name, etc. Get onfig data from config file.
        self.__mapper_files = []
        self.__combiner_files = []
        self.__reducer_files = []
        self.gcp_api = gcp.GCP_API()

    def init_cluster(self, num_mapper, num_reducer):
        # Check and run the key value store if not already running.
        # Create number of workers based on the requirement, store the value in self.__worker_list, The list would contain the xmlrpc client of each worker node, connected through ip
        # after creating the instance.
        global config
        self.__num_mapper = num_mapper
        self.__num_reducer = num_reducer

        # Check if kv_store server is running.
        try:
            logging.info('Checking for already existing key value store.')
            _, external_ip = self.gcp_api.getIPAddresses(parser["GCP"]["project"], parser["GCP"]["zone"], parser["kv_store"]["name"])
        except:
            logging.info('Creating a new key value store instance.')
            _, external_ip = self.gcp_api.create_instance(parser["GCP"]["project"], parser["GCP"]["zone"], parser["kv_store"]["name"])

        print('external ip', external_ip)
        while True:
            try:
                print("http://{0}:{1}/".format(external_ip, parser["kv_store"]["port"]))
                kv_client = xmlrpc.client.ServerProxy("http://{0}:{1}/".format(external_ip, parser["kv_store"]["port"]))
                if kv_client.getStatus() == 'OK':
                    logging.info('Key store status OK.')
                    break
            except:
                logging.exception("Could not connect to key value store, trying again!")
                time.sleep(10)
                continue
        
        config["kv_client"] = {}
        config["kv_client"]["rpc_client"] = kv_client
        config["kv_client"]["ip"] = external_ip

        # Create worker nodes based on the number of mapper and reducer instances.
        self.__worker_instance_count = self.__num_mapper if self.__num_mapper > self.__num_reducer else self.__num_reducer

        config["worker_client"] = {}

        for i in range(self.__worker_instance_count):

            logging.info('Creating worker node instance {0}.'.format(str(i)))
            worker_name = parser["worker"]["name"] + str(i)
            _, external_ip = self.gcp_api.create_instance(parser["GCP"]["project"], parser["GCP"]["zone"], worker_name)
            print('external', external_ip)
            # time.sleep(5)
            while True:
                try:
                    print('before worker client')
                    worker_client = xmlrpc.client.ServerProxy("http://{0}:{1}/".format(external_ip, parser["worker"]["port"]))
                    print('worker client', worker_client)
                    print(config['kv_client']['ip'])
                    
                    if worker_client.getStatus(config["kv_client"]["ip"]) == 'OK':
                        break
                except Exception as e:
                    print(str(e))
                    logging.info('Waiting for worker node {0} server to respond.'.format(str(i)))
                    time.sleep(10)
                    continue
            
            config["worker_client"][str(i)] = external_ip
        
        print(config)
        return time.time()
        
    def genHash(self, word):
        hash_val = hash(word)
        return hash_val%self.hashLength

    def flatMapWords(self, grouped_words):
        final_map = {}
        for word_tuple in grouped_words:
            if word_tuple[0] in final_map:
                final_map[word_tuple[0]].append(word_tuple[1])
            else:
                final_map[word_tuple[0]] = [word_tuple[1]]
        # print(final_map)
        return final_map
    
    def __combine_mapper(self):
        global config
        data_blocks = []
        self.hashLength = self.__num_reducer
        key_store = config["kv_client"]["rpc_client"]

        try:
            # with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
            for map_file in self.__mapper_files:
                map_key = map_file.split('.')[0]
                
                get_str = 'get ' + map_file + ' ' + map_key
                raw_data = key_store.mapReduceHandler(get_str)
                raw_data = raw_data.split('\n')[0]
                if raw_data != 'error_get':
                    data = json.loads(raw_data)
        
                    [data_blocks.append(val) for val in data[map_key]]
                else:
                    logging.warning('Error data fetch in combiner')
        except Exception as e:
            logging.exception('Exception raised connecting to key store in combiner' + str(e))

        # Call function to combine similar words.
        grouped_words = {}
        print(data_blocks[0])
        for word_tuple in data_blocks:
            hash_output = self.genHash(word_tuple[0])
            if hash_output in grouped_words:
                grouped_words[hash_output].append(word_tuple)
            else:
                grouped_words[hash_output] = [word_tuple]

        print("Done hashing")
        logging.info('Done hashing files in combiner')
        
        try:
            # with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
            for keys in grouped_words:
                json_file = 'intermediate_{0}.json'.format(keys)
                self.__combiner_files.append(json_file)
                key = 'intermediate_{0}'.format(keys)
                data = {}
                # grouped_words[keys].sort()
                final_map = self.flatMapWords(grouped_words[keys])
                data[key] = final_map
                send_str = 'set {0} {1} {2} \n{3}\n'.format(json_file, key, len(data), json.dumps(data))
                res = key_store.mapReduceHandler(send_str)
                print(res)
                logging.info('Combiner result: '+ res)
        except Exception as e:
            logging.exception('Exception raised connecting to key store in combiner' + str(e))
    
    def __split_input_file(self, input_path):
        data = open(input_path, 'r', encoding='utf-8').readlines()

        input_file_lines = ''

        for line in data:
            line = line.translate(line.maketrans(string.punctuation, ' ' * len(string.punctuation)))
            for word in line.split():
                if word.isalpha():
                    input_file_lines += word + ' '
        
        self.__chunk_length = len(input_file_lines)//self.__num_mapper

        return input_file_lines

    def run_mapred(self, input_path, map_func, red_func, output_path):
        # Call input processing function to split the input according to teh number of workers.
        global config
        key_store = config["kv_client"]["rpc_client"]
        
        if os.path.isfile(input_path):
            file_list = {}
            formatted_string = self.__split_input_file(input_path)

            for i in range(self.__num_mapper):
                filename = 'initial_'+str(i)+'.json'
                self.__mapper_files.append('mapper_'+str(i)+'.json')
                file_list[str(i)] = [filename]
                if i != self.__num_mapper-1:
                    map_data = formatted_string[i*self.__chunk_length:(i+1)*self.__chunk_length]
                else:

                    map_data = formatted_string[i*self.__chunk_length:]

                # Store the result in the key value store for mapper to consume and append it to the mapper files list. use filename as key.
                # with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                key = filename.split('.')[0]
                payload = {}
                payload[key] = map_data
                send_str = 'set {0} {1} {2} \n{3}\n'.format(filename, key, len(map_data), json.dumps(payload))
                res = key_store.mapReduceHandler(send_str)
                print(res)
                logging.info('Initial data storage result: ' + res)

            #TODO: initialize worker list in init_cluster with the connection to each worker VM instance created.

        elif os.path.isdir(input_path):
            file_list = {}
            folder_data = glob.glob(input_path+"/*.txt")
            # print('folder data: ' + ''.join(folder_data))

            for idx, file_path in enumerate(folder_data):
                hash_val = idx % self.__num_mapper
                file_name = file_path.split('.')[0].split('\\')[-1]+'.json'
                
                filename = 'mapper_{0}.json'.format(file_name.split('.')[0])
                self.__mapper_files.append(filename)

                formatted_string = self.__split_input_file(file_path)
                # Store the file in key value store  with the file name and formatted string.

                if str(hash_val) in file_list:
                    file_list[str(hash_val)].append(file_name)
                else:
                    file_list[str(hash_val)] = [file_name]

                # with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                key = file_name.split('.')[0]
                payload = {}
                payload[key] = formatted_string
                send_str = 'set {0} {1} {2} \n{3}\n'.format(file_name, key, len(formatted_string), json.dumps(payload))
                res = key_store.mapReduceHandler(send_str)
                print(res)
                logging.info('Initial data storage result: ' + res)

        # self.__worker_list = []
        # obj = xmlrpc.client.ServerProxy("http://localhost:8000/", allow_none=True)
        # for i in range(self.__num_mapper):
        #     self.__worker_list.append(obj)

        self.file_list = file_list

        tasks = []
        for file in file_list:
            print(file_list[file])

        for idx in range(self.__num_mapper):
            mapper_ip = config["worker_client"][str(idx)]
            mapper_port = 3389
            p = Process(target=mapperWorker, args=(map_func, idx, self.file_list, mapper_ip, mapper_port, ))
            p.start()
            tasks.append(p)

        for task in tasks:
            task.join()
        
        print('Done Mapper')
        logging.info('Mapper work done')
        # Write logic to send the file names into different modules based on their hash value, or else their mapper name.

        # Starting logic for combiner code in master. Try to find logic to generate the filenames stored in the key value store.
        self.__combine_mapper()

        # Functions to take care for the reducer.
        print('Done combining mapper ouput.')
        logging.info('Combiner work done')
        print(self.__combiner_files)
        tasks = []
        for i in range(self.__num_reducer):
            reducer_ip = config["worker_client"][str(i)]
            reducer_port = 3389
            print('Inside reducer call process generation')
            logging.info('Inside reducer call process generation')
            p = Process(target=reducerWorker, args=(red_func, i, self.__combiner_files, reducer_ip, reducer_port, ))
            p.start()
            tasks.append(p)
        for task in tasks:
            task.join()
        
        print('Combining outputs from reducer')
        logging.info('Combining outputs from reducer')

        for i in range(self.__num_reducer):
            self.__reducer_files.append('reducer_{0}.json'.format(i))

        result = {}

        # with xmlrpc.client.ServerProxy("http://{0}:{1}/".format(parser['kvstore']['ip'], int(parser['kstoore']['port']))) as key_store:
        for file in self.__reducer_files:
            key = file.split('.')[0]
            get_str = 'get ' + file + ' ' + key
            raw_data = key_store.mapReduceHandler(get_str)
            raw_data = raw_data.split('\n')[0]

            json_data = json.loads(raw_data)
            # [result.append(val) for val in json_data[key]]
            result[key] = json_data[key]

        final = json.dumps(result, sort_keys=True, indent=0, separators=(',', ':'))

        kv_store = config["kv_client"]["rpc_client"]
        set_str = 'setOp {0} {1} {2} \n{3}\n'.format(output_path, key, len(final), final)

        try:
            res = kv_store.mapReduceHandler(set_str)
            
        except:
            logging.error("Error in file put to key store for final output.")
            print( 'Error in file put to key store for final output.')

        # with open(output_path, 'w+') as f:
        #     f.write(final)
        logging.info('Completed map reduce function for ' + map_func)

        return final

    def destroy_cluster(self, ):
        global config

        logging.info('Instructing key store to flush the created files')
        while True:
            status = config["kv_client"]["rpc_client"].flushFiles()
            if status == 'OK':
                break

        logging.info('Destroying created worker nodes from the cluster')
        for i in range(self.__worker_instance_count):
            worker_name = parser["worker"]["name"]+str(i)
            self.gcp_api.delete_instance(parser["GCP"]["project"], parser["GCP"]["zone"], worker_name)

if __name__ == "__main__":
    parser = ConfigParser()
    parser.read('config.ini')
  
    logging.basicConfig(filename='master.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    master = MasterServer()
    
    try:
        print('Starting master node RPC server')
        logging.info('Starting master node RPC server')
        server = SimpleXMLRPCServer((parser['master']['ip'], int(parser['master']['port'])), allow_none=True)
        server.register_instance(master)
        logging.info('Started master node server, running at: ' + str(parser.get('master', 'ip') + ':' + str(parser.get('master', 'port'))))
        server.serve_forever()
    except Exception as e:
        logging.exception('Error creating master server with exception ' + str(e))