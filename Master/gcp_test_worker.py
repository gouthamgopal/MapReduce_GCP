import gcp
import configparser
import xmlrpc.client

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    master = xmlrpc.client.ServerProxy('http://localhost:3389')
    master.init_cluster(2, 2)
    # result = master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index\Galatea.txt", 'wordcount', 'wordcount', 'wordcount_output.txt')
    result = master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index", 'invertedindex', 'invertedindex', 'invertedIndex_output.txt')
    master.destroy_cluster()

    print(result)