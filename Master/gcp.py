import configparser
import os
import time
import json

import googleapiclient.discovery
from google.oauth2 import service_account


class GCP_API:
    
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini', encoding = 'utf8')
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform']
        self.sa_file = self.config['GCP']['cred_file_path']
        credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes=self.scopes)
        self.service = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        
    def create_instance(self, project, zone, name):
        # snap_name = server_instance_name + '-snap'
        # if name == self.config['KVServer']['kvserver_name']:
        #     snap_name = self.config['KVServer']['kvserver_snap']

        # try:
        #     snap = self.service.snapshots().get(project = project, snapshot = snap_name).execute()
        # except:
        #     snap = self.create_snapshot(project, zone, server_instance_name, snap_name)
        # source_snap = snap['selfLink']
        
        machine_type = "zones/" + zone + "/machineTypes/" + self.config['GCP']['machine_type']
        if name == self.config['kv_store']['kvserver_name']:
            startup_script = open(os.path.join(os.path.dirname(__file__), self.config['kv_store']['kvserver_startup_script_path']), 'r').read()
        elif name == self.config['master']['master_server']:
            startup_script = open(os.path.join(os.path.dirname(__file__), self.config['master']['master_startup_script_path']), 'r').read()
        else:
            startup_script = open(os.path.join(os.path.dirname(__file__), self.config['worker']['worker_startup_script_path']), 'r').read()
        
        # print(startup_script)
        
        imageSource = self.service.images().getFromFamily(project=self.config['GCP']['image_project'], family=self.config['GCP']['image_family']).execute()
        imageSourceLink = imageSource['selfLink']

        config = {
            'name': name,
            'machineType': machine_type,
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': { 
                        'sourceImage': imageSourceLink
                    }
                }
            ],
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }]
            }
        }
        
        while True:
            try:
                print("Creating instance.")
                instance = self.service.instances().insert(project = project, zone = zone, body = config).execute()
                self.wait_for_operation(project, zone, instance['name'], 'start_vm')
                break
            except Exception as e:
                print(str(e))
                continue
        
        return self.getIPAddresses(project, zone, name)

    def delete_instance(self, project, zone, name):
        # credentials = service_account.Credentials.from_service_account_file(self.sa_file, scopes = self.scopes) 
        # compute = googleapiclient.discovery.build('compute', 'v1', credentials = credentials)
        while True:
            try:
                instance = self.service.instances().delete(project = project, zone = zone, instance = name).execute()
                self.wait_for_operation(project, zone, instance['name'], 'delete_vm')
                break
            except Exception as e:
                print(str(e))
                continue
    

    def wait_for_operation(self, project, zone, operation, action):
        print('Waiting for ' + action + ' operation to finish... ', end = '')
        while True:
            result = self.service.zoneOperations().get(
                project=project,
                zone=zone,
                operation=operation).execute()
    
            if result['status'] == 'DONE':
                print("DONE", end = '\n')
                if 'error' in result:
                    raise Exception(result['error'])
                return result
    
            time.sleep(1)
    

    def getIPAddresses(self, project_id, zone, name):
        instance = self.service.instances().get(project=project_id, zone=zone, instance=name).execute()
        external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        internal_ip = instance['networkInterfaces'][0]['networkIP']
        return internal_ip, external_ip
