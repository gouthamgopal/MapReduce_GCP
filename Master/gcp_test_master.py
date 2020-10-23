import gcp
import configparser
import xmlrpc.client

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    gcp_api = gcp.GCP_API()
    master = xmlrpc.client.ServerProxy('http://localhost:3389')
    master.init_cluster(3, 4)
    master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index\Galatea.txt", 'wordcount', 'wordcount', 'Output/wordcount_output.txt')
    master.run_mapred(r"D:\IUB\IU_Fall_20\Cloud_Computing\Map_reduce\Master\inverted_index", 'invertedindex', 'invertedindex', 'Output/invertedIndex_output.txt')
    