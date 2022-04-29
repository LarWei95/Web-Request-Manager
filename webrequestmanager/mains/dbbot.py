'''
Created on 12.04.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage
from webrequestmanager.control.requesthandling import RequestHandler
from webrequestmanager.control.requester import Requester
import json
import time
import datetime as dt

def main():
    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "localhost"
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage(host, user, password)
    
    timeout_default = dt.timedelta(hours=3)
    
    # proxy_manager = ProxyManager(HideMyNameProxyList())
    proxy_manager = None
    requester = Requester(proxy_manager)
    
    request_handler = RequestHandler(storage, requester, timeout_default=timeout_default)
    
    wait = False
    
    while True:
        if wait:
            time.sleep(5.0)
        
        wait = not request_handler.execute_requests()

if __name__ == '__main__':
    main()
