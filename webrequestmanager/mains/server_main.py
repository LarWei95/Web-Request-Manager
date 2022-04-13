'''
Created on 12.04.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage
from webrequestmanager.control.api import WebRequestAPIServer
from webrequestmanager.control.requester import Requester
import json


def run_api (host, user, password):
    storage = Storage(host, user, password)
    
    # proxy_manager = ProxyManager(HideMyNameProxyList())
    proxy_manager = None
    requester = Requester(proxy_manager)
    
    server = WebRequestAPIServer(requester, storage)
    server.run()

def main ():
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "localhost"
    user = credentials["user"]
    password = credentials["password"]
    
    run_api(host, user, password)
    
if __name__ == '__main__':
    main()