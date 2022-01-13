'''
Created on 06.01.2022

@author: larsw
'''
from model.storage import Storage, URL, RequestHeader, Request, Response
from control.requesthandling import RequestHandler
import datetime as dt
import requests
from pprint import pprint
import json

def test ():
    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage("localhost", user, password)
    request_handler = RequestHandler(storage)
    
    urls = [
            "https://www.w3resource.com/mysql/comparision-functions-and-operators/not-in.php",
            "https://www.idealo.de/",
            "http://fritz.box/",
            "https://doc.cgal.org/latest/Straight_skeleton_2/index.html"
        ]
    headers = [
            {}
            for _ in urls
        ]
    
    min_date = dt.datetime(2022, 1, 13, 14)
    
    request_ids = []
    
    for url, header in zip(urls, headers):
        request_id = request_handler.add_request(url, header, min_date)
        request_ids.append(request_id)
    
    without_request = request_handler.execute_requests()
    

def req ():
    url = "https://docs.python-requests.org/en/latest/"
    response = requests.get(url)
    
    pprint(dir(response), indent=3)
    print(response.status_code)
    print(response.headers)

if __name__ == '__main__':
    test()
    
    