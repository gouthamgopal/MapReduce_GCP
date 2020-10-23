import gcp
import configparser
import xmlrpc.client

if __name__ == '__main__':
    # parser = configparser.ConfigParser()
    # parser.read('config.ini')
    # master = xmlrpc.client.ServerProxy('http://localhost:3389')
    # master.init_cluster(2, 2)
    # master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index\Galatea.txt", 'wordcount', 'wordcount', 'Output/wordcount_output.txt')
    # master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index", 'invertedindex', 'invertedindex', 'Output/invertedIndex_output.txt')
    worker = xmlrpc.client.ServerProxy('http://35.239.127.200:3389/')
    print(worker.getStatus('35.222.99.158'))
    