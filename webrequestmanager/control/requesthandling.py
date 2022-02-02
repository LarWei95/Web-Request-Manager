'''
Created on 13.01.2022

@author: larsw
'''
from webrequestmanager.model.storage import URL, RequestHeader, Request, Response
import datetime as dt
import json
import requests

class RequestHandler():
    '''
    classdocs
    '''
    def __init__(self, storage, timeout_default=dt.timedelta(hours=3)):
        self._storage = storage
        self._session = requests.Session()
        
        self._timeout_default = timeout_default
        
    def add_request (self, url, headers={}, min_date=None, max_date=None):
        url = URL.of_string(url)
        headers = RequestHeader.of_dict(headers)
        
        now = dt.datetime.now()
        request = Request(url, headers, now)
        
        request_id = self._storage.insert_request(request,
                                                  min_date=min_date,
                                                  max_date=max_date)
        return int(request_id)
    
    def add_response (self, request_id, request, timestamp, requests_response):
        response = Response.of_response(request, requests_response)
        response_id = self._storage.direct_insert_response(request_id, 
                                                           timestamp, 
                                                           response)
        return response_id
    
    def get_response (self, url=None, headers={}, min_date=None, max_date=None, request_id=None):
        if url is None and request_id is None:
            errmsg = "Both URL and request id are None."
            raise ValueError(errmsg)
        
        if request_id is None:
            request_id = self.add_request(url, headers, min_date, max_date)
        
        latest_response = self._storage.get_latest_response (request_id, status_code=200)
        return latest_response
    
    def _execute_web_request (self, request_index, url, header):
        d = dt.datetime.now()
        
        with self._session as s:
            requests_response = s.get(url, headers=header)
            
        response = Response.of_response(None, requests_response)
        self._storage.direct_insert_response(request_index, d, response)
    
    def _execute_pending_requests (self):
        df = self._storage.get_requests_without_responses()
        
        for indx in df.index.get_level_values("RequestId"):
            row = df.xs(indx, axis=0, level="RequestId").iloc[0]
            header = json.loads(row.loc["Header"])
            url = row.loc["URL"]
            
            self._execute_web_request(indx, url, header)
    
    def fill_default_domain_timeouts (self):
        unset_domain_ids = self._storage.get_domain_ids_without_domain_timeouts()
        timeouts = [
                self._timeout_default
                for _ in unset_domain_ids
            ]
        
        self._storage.direct_insert_domain_timeout(unset_domain_ids, timeouts)
    
    def execute_failing_requests (self):
        df = self._storage.get_retryable_failing_request()
        
        print(df)
        
        for request_id in df.index.get_level_values("RequestId"):
            row = df.xs(request_id, axis=0, level="RequestId").iloc[0]
            
            header = json.loads(row.loc["Header"])
            url = row.loc["URL"]
            
            self._execute_web_request(request_id, url, header)
    
    def execute_requests (self):
        self._execute_pending_requests()
        self.fill_default_domain_timeouts()
        self.execute_failing_requests()
        