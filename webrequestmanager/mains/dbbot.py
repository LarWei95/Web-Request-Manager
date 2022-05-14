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

class DBBotRunner ():
    def __init__ (self, request_handler, wait_timelist):
        self._request_handler = request_handler
        self._wait_timelist = list(wait_timelist)
        
        self._wait_index = -1


    def do_iteration (self):
        if self._wait_index != -1:
            time.sleep(self._wait_timelist[self._wait_index])

        increment_waiting = not self._request_handler.execute_requests()

        if increment_waiting:
            if self._wait_index < (len(self._wait_timelist) - 1):
                self._wait_index += 1
        else:
            if self._wait_index > -1:
                self._wait_index = -1



def main():
    wait_seconds = [60.0, 120.0, 240.0, 480.0, 900.0]

    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "192.168.178.21"
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage(host, user, password)
    
    timeout_default = dt.timedelta(hours=3)
    
    # proxy_manager = ProxyManager(HideMyNameProxyList())
    proxy_manager = None
    requester = Requester(proxy_manager)
    
    request_handler = RequestHandler(storage, requester, timeout_default=timeout_default)
    
    runner = DBBotRunner(request_handler, wait_seconds)
    
    while True:
        runner.do_iteration()

if __name__ == '__main__':
    main()
