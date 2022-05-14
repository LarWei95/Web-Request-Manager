'''
Created on 12.04.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage
from webrequestmanager.control.api import WebRequestAPIServer
from webrequestmanager.control.requester import Requester
import json

def main ():
    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "192.168.178.21"
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage(host, user, password)
    
    # storage.direct_insert_domain_policy(3, retries=5, retry_mindelay=1, retry_maxdelay=5)
    # storage.direct_insert_domain_policy(4, retries=5, retry_mindelay=1, retry_maxdelay=5)
    print(storage.get_domain_policy())
if __name__ == '__main__':
    main()
