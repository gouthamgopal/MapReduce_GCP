import configparser
import xmlrpc.client
import gcp

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    gcp_api = gcp.GCP_API()
    gcp_api.create_instance(parser['GCP']['project'], parser['GCP']['zone'], 'master-server')
    print('Instance created')
    internal, external = gcp_api.getIPAddresses(parser['GCP']['project'], parser['GCP']['zone'], 'master-server')
    print('internal', internal)
    print('external', external)
    