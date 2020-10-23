#!/usr/local/bin/python3
import socket
import sys
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import json
import re
import logging


class Worker:
    def __init__(self):
        pass

    def map_wordcount(self, filename, index):
        print('Inside mapper wordcount')
        try:
            output = []
            key = filename.split('.')[0]
            with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                get_str = 'get ' + filename + ' ' + key
                print('mapper query string: '+ get_str)
                raw_data = key_store.mapReduceHandler(get_str)
                raw_data = raw_data.split('\n')[0]
                json_data = json.loads(raw_data)

                for word in json_data[key].split():
                    word = word.lower()
                    if word.isalpha():
                        output.append((word, 1))

                    data = {}
                    key = "mapper_"+str(index)
                    json_file = 'mapper_'+str(index)+'.json'
                    data[key] = output

                with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                    send_str = 'set {0} {1} {2} \n{3}\n'.format(
                        json_file, key, len(data), json.dumps(data))
                    res = key_store.mapReduceHandler(send_str)
                    if res.split(' ')[0] == 'STORED':
                        return json_file
                    else:
                        logging.warning('Error response generated while key store dump in wordcount mapper.')
                        return 'error_response'

                logging.warning('Error response generated while conecting to key store in wordcount mapper.')
                return 'error_response'

        except Exception as e:
            print(str(e))
            logging.exception('Exception raised in wordcount mapper: '+ str(e))
            logging.info('Returning from wordcount mapper since error in connection with key store server.')
            return 'error in connection.'

    def map_invertedindex(self, filename, index):
        # for file_name in filename:
        print('{0} filename is getting mapped'.format(filename))
        try:
            output = []

            # Change this to accpet the key and access value from key value store.
            # with open(filename, 'r', encoding='utf-8') as f:
            key = filename.split('.')[0]

            with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                get_str = 'get ' + filename + ' ' + key
                print('mapper query string: '+ get_str)
                raw_data = key_store.mapReduceHandler(get_str)
                # print('rwa data:', raw_data)
                raw_data = raw_data.split('\n')[0]
                # print('raw data formatted:', raw_data)
                json_data = json.loads(raw_data)

                file_name = key
                print(len(json_data[key]))

                for word in json_data[key].split():
                    word = word.lower()
                    if word.isalpha():
                        output.append((word, file_name))
                    # words = re.findall(r'\w+', line.strip())
                    # for word in words:

            data = {}
            # key = filename.split('.')[0].lower()
            json_file = "mapper_{0}.json".format(filename.split('.')[0])
            key = json_file.split('.')[0]
            data[key] = output

            with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
                send_str = 'set {0} {1} {2} \n{3}\n'.format(
                    json_file, key, len(data), json.dumps(data))
                res = key_store.mapReduceHandler(send_str)
                if res.split(' ')[0] == 'STORED':
                    return json_file
                else:
                    logging.warning('Error response generated while key store dump in inverted index mapper.')
                    return 'error_response'

            logging.warning('Error response generated while conecting to key store in inverted index mapper.')
            return 'error_response'

        except Exception as e:
            print(str(e))
            logging.exception('Exception raised in inverted index mapper: '+ str(e))
            logging.info('Returning from inverted index mapper since error in connection with key store server.')
            return 'error in connection.'

    def reducer_wordcount(self, data, key, index):
        reduced_count = []
        word_res = data[key]

        for key in word_res:
            reduced_count.append([key, sum(word_res[key])])

        json_file = 'reducer_{0}.json'.format(str(index))
        json_key = json_file.split('.')[0]

        final = {}
        final[json_key] = reduced_count
        with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
            send_str = 'set {0} {1} {2} \n{3}\n'.format(
                json_file, json_key, len(final), json.dumps(final))
            res = key_store.mapReduceHandler(send_str)
            print(res)

    def reducer_invertedindex(self, data, key, index):
        reduced_words = {}
        word_res = data[key]

        # for keys in word_res:
        for key in word_res:
            file_per_word = {}

            for file_name in word_res[key]:
                filename = str(file_name).split('_')[0]+'.txt'
                if filename == '1.txt':
                    continue
                if filename in file_per_word:
                    file_per_word[filename] += 1
                else:
                    file_per_word[filename] = 1
            reduced_words[key] = file_per_word

        json_file = 'reducer_{0}.json'.format(index)
        json_key = json_file.split('.')[0]

        final = {}
        final[json_key] = reduced_words
        with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
            send_str = 'set {0} {1} {2} \n{3}\n'.format(
                json_file, json_key, len(final), json.dumps(final))
            res = key_store.mapReduceHandler(send_str)
            return res

    def reducer_helper(self, filename, red_func, index):

        with xmlrpc.client.ServerProxy("http://localhost:8001/") as key_store:
            intermediate_key = filename.split('.')[0]
            get_str = 'get ' + filename + ' ' + intermediate_key
            print(get_str)
            logging.info('Reducer get string: ' + get_str)
            raw_data = key_store.mapReduceHandler(get_str)
            raw_data = raw_data.split('\n')[0]
            if raw_data != 'error_get':
                data = json.loads(raw_data)
                if red_func == 'wordcount':
                    return(self.reducer_wordcount(data, intermediate_key, index))
                else:
                    return(self.reducer_invertedindex(data, intermediate_key, index))
            else:
                logging.error('Fetch error for reducer input data.')
                return 'error_fetch'
                


def main():
    logging.basicConfig(filename='worker.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    print("Starting mapper connection..")
    logging.info('Starting worker server connection.')
    worker = Worker()

    server = SimpleXMLRPCServer(("localhost", 8000), allow_none=True)

    server.register_instance(worker)
    server.serve_forever()


if __name__ == "__main__":
    main()
