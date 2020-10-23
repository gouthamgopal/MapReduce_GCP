import gcp
import configparser

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    gcp_api = gcp.GCP_API()
    gcp_api.create_instance(parser['GCP']['project'], parser['GCP']['zone'], 'worker-vm')
    print('Instance created')
    internal, external = gcp_api.getIPAddresses(parser['GCP']['project'], parser['GCP']['zone'], 'worker-vm')
    print('internal', internal)
    print('external', external)
    # gcp_api.delete_instance(parser['GCP']['project'], parser['GCP']['zone'], 'worker-vm')
    # print('Instance deleted')
    