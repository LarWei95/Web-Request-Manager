'''
Created on 06.01.2022

@author: larsw
'''
from webrequestmanager.model.storage import Storage, URL, RequestHeader, Request, Response
from webrequestmanager.control.requesthandling import RequestHandler
from webrequestmanager.control.api import WebRequestAPIClient
import datetime as dt
import requests
from pprint import pprint
import json

def fulltest ():
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage("localhost", user, password)
    request_handler = RequestHandler(storage)
    
    urls = [
            "https://www.w3resource.com/mysql/comparision-functions-and-operators/not-in.php",
            "https://www.idealo.de/",
            "http://fritz.box/",
            "https://doc.cgal.org/latest/Straight_skeleton_2/index.html",
            "https://www.w3resource.com/mysql/comparision-functions-and-operators/not-in.php",
            "https://www.idealo.de/",
            "http://fritz.box/",
            "https://doc.cgal.org/latest/Straight_skeleton_2/index.html"
        ]
    headers = [
            {}
            for _ in range(len(urls) // 2)
        ] + [
            {
                "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding" : "gzip, deflate, br",
                "Accept-Language" : "de,en-US;q=0.7,en;q=0.3",
                "Cache-Control" : "max-age=0",
                "Connection" : "keep-alive",
                "Host" : "www.idealo.de",
                "Sec-Fetch-Dest" : "document",
                "Sec-Fetch-Mode" : "navigate",
                "Sec-Fetch-Site" : "same-origin",
                "TE" : "trailers",
                "Upgrade-Insecure-Requests" : "1",
                "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0"
            }
            for _ in range(len(urls) // 2)
        ]
    
    min_date = dt.datetime(2022, 1, 13, 14)
    
    request_ids = []
    
    for url, header in zip(urls, headers):
        request_id = request_handler.add_request(url, header, min_date)
        request_ids.append(request_id)
    
    request_handler.execute_requests()
    print(storage.get_domain_status())
    
    for url, header in zip(urls, headers):
        print(request_handler.get_response(url, header, min_date))
    
    # storage.direct_insert_domain_timeout(1, dt.timedelta(days=2, hours=12, minutes=3))
    print(storage.get_latest_domain_timeout())
    
def execute_failing_test ():
    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage("localhost", user, password)
    request_handler = RequestHandler(storage)
    
    request_handler.execute_failing_requests()
    
def req ():
    url = "https://docs.python-requests.org/en/latest/"
    response = requests.get(url)
    
    pprint(dir(response), indent=3)
    print(response.status_code)
    print(response.headers)

def client ():
    url = "https://docs.python-requests.org/en/latest/user/quickstart/"
    header = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"}
    min_date = dt.datetime(2022, 1, 1)
    max_date = dt.datetime(2022, 3, 1)
    
    
    client = WebRequestAPIClient("http://127.0.0.1", 5000)
    request_id = client.post_page_request(url, header, min_date, max_date)
    print(request_id)
    # EXECUTE NECESSARY!
    
    response = client.get_response(request_id)
    pprint(response)

def quick_execute ():
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage("localhost", user, password)
    request_handler = RequestHandler(storage)
    request_handler.execute_requests()

if __name__ == '__main__':
    client()
    
    