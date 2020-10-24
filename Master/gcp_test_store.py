import gcp
import configparser

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    gcp_api = gcp.GCP_API()
    gcp_api.create_instance(parser['GCP']['project'], parser['GCP']['zone'], 'kv-store-server')
    print('Instance created')
    internal, external = gcp_api.getIPAddresses(parser['GCP']['project'], parser['GCP']['zone'], 'kv-store-server')
    print('internal', internal)
    print('external', external)
    