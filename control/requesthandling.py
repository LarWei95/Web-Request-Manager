'''
Created on 13.01.2022

@author: larsw
'''
from model.storage import URL, RequestHeader, Request, Response
import datetime as dt
from urllib.parse import urlparse
import json
import requests
import gzip
from io import BytesIO

class RequestHandler():
    '''
    classdocs
    '''
    def __init__(self, storage):
        self._storage = storage
        self._session = requests.Session()
        
    def add_request (self, url, headers={}, min_date=None, max_date=None):
        url = URL.of_string(url)
        headers = RequestHeader.of_dict(headers)
        
        now = dt.datetime.now()
        request = Request(url, headers, now)
        
        request_id = self._storage.insert_request(request,
                                                  min_date=min_date,
                                                  max_date=max_date)
        return request_id
    
    def add_response (self, request_id, request, timestamp, requests_response):
        response = Response.of_response(request, requests_response)
        response_id = self._storage.direct_insert_response(request_id, 
                                                           timestamp, 
                                                           response)
        return response_id
    
    def get_response (self, url, headers={}, min_date=None, max_date=None):
        request_id = self.add_request(url, headers, min_date, max_date)
        print("Request ID: "+str(request_id))
    
    def _execute_pending_requests (self):
        df = self._storage.get_requests_without_responses()
        
        print(df)
        
        for indx in df.index.values:
            row = df.loc[indx]
            
            header = json.loads(row.loc["Header"])
            url = row.loc["URL"]
            
            with self._session as s:
                requests_response = s.get(url, headers=header)
                
            d = dt.datetime.now()
            response = Response.of_response(None, requests_response)
            
            self._storage.direct_insert_response(indx, d, response)

    def _execute_failing_requests (self):
        df = self._storage.get_requests_without_coded_responses(200)
        print(df)
        
        for request_id in df.index.values:
            row = df.loc[request_id]
            
            header = json.loads(row.loc["Header"])
            url = row.loc["URL"]
            
            # latest_response = self._storage.get_latest_response(request_id)
            
            with self._session as s:
                requests_response = s.get(url, headers=header)
                
            d = dt.datetime.now()
            response = Response.of_response(None, requests_response)
            
            self._storage.direct_insert_response(request_id, d, response)
    
    def execute_requests (self):
        self._execute_pending_requests()
        self._execute_failing_requests()
        