'''
Created on 01.02.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage
from webrequestmanager.control.api import WebRequestAPIServer
import json

if __name__ == '__main__':
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage("localhost", user, password)
    server = WebRequestAPIServer(storage)
    server.run()