'''
Created on 01.02.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage
from webrequestmanager.control.api import WebRequestAPIServer
from webrequestmanager.control.requesthandling import RequestHandler
import json
import multiprocessing as mp
import time
import datetime as dt
import traceback as tb

def run_api (host, user, password):
    try:
        storage = Storage(host, user, password)
        server = WebRequestAPIServer(storage)
        server.run()
    except:
        tb.print_exc()
    

if __name__ == '__main__':
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "localhost"
    user = credentials["user"]
    password = credentials["password"]
    
    pool = mp.Pool(1)
    pool.apply_async(run_api, (host, user, password))
    
    storage = Storage(host, user, password)
    
    timeout_default = dt.timedelta(seconds=5)
    request_handler = RequestHandler(storage, timeout_default=timeout_default)
    
    wait = False
    
    while True:
        if wait:
            time.sleep(5.0)
        
        wait = not request_handler.execute_requests()