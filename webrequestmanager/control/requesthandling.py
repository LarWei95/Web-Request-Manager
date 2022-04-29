'''
Created on 13.01.2022

@author: larsw
'''
from webrequestmanager.model.storage import URL, RequestHeader, Request, Response
import datetime as dt
import json
import requests
import time
import numpy as np
import pandas as pd
import logging

class RequestHandler():
    '''
    classdocs
    '''
    def __init__(self, storage, requester, timeout_default=dt.timedelta(hours=3)):
        self._storage = storage
        # self._session = requests.Session()
        self._requester = requester
        
        self._timeout_default = timeout_default
        
        self._logger = logging.getLogger("requesthandler")
        self._logger.setLevel(logging.INFO)
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        
        self._logger.addHandler(ch)
        
        
    def add_request (self, url, headers={}, accepted_status=200, min_date=None, max_date=None):
        url = URL.of_string(url)
        headers = RequestHeader.of_dict(headers)
        
        now = dt.datetime.now()
        request = Request(url, headers, now, accepted_status)
        
        request_id = self._storage.insert_request(request,
                                                  min_date=min_date,
                                                  max_date=max_date)
        return int(request_id)
    
    def add_response (self, request_id, request, requests_response):
        response = Response.of_response(request, requests_response, dt.datetime.now())
        response_id = self._storage.direct_insert_response(request_id,
                                                           response)
        return response_id
    
    def get_response (self, url=None, headers={}, min_date=None, max_date=None, request_id=None):                
        if url is None and request_id is None:
            errmsg = "Both URL and request id are None."
            raise ValueError(errmsg)
        
        if request_id is None:
            request_id = self.add_request(url, headers, min_date, max_date)
        
        latest_response = self._storage.get_latest_accepted_response(request_id)
        return latest_response
    
    def _execute_web_request (self, request_index, url, header, accepted_status_codes):
        '''
        with self._session as s:
            requests_response = s.get(url, headers=header, allow_redirects=False)
        '''
        
        self._logger.info("Requesting: {:s}".format(str(url)))
        requests_response = self._requester.request(url, header, accepted_status_codes)
        
        
        if requests_response is not None:
            response = Response.of_response(None, requests_response, dt.datetime.now())
            self._storage.direct_insert_response(request_index, response)
            self._logger.info("Got a useable response.")
            return response.is_accepted(accepted_status_codes)
        else:
            self._logger.info("Got no useable response.")
        
        return False            
        
    def _split_fullrequest_dataframe_by_domain (self, df, count):
        '''
        FULLREQUEST_COLUMNS = ["RequestId", "UrlId", "DomainId", "HeaderId", "Scheme", "Netloc",
                           "Path", "Query", "Header", "Timestamp", "URL"]
        FULLREQUEST_INDEX = ["RequestId", "UrlId", "DomainId", "HeaderId"]
        '''
        
        splitted = []
        
        if len(df) != 0:
            unique_domain_ids = df.index.get_level_values("DomainId").unique()
            
            for unique_domain_id in unique_domain_ids:
                subdf = df.xs(unique_domain_id, axis=0, level="DomainId", drop_level=False)[:count]
                splitted.append(subdf)
        
            splitted = pd.concat(splitted, axis=0)
            rnd = np.arange(len(splitted))
            np.random.shuffle(rnd)
            
            splitted = splitted.iloc[rnd]
        
        return splitted
        
    def _execute_pending_requests (self):
        self._logger.info("Start PendingRequests")
        df = self._storage.get_requests_without_responses()
        df = self._split_fullrequest_dataframe_by_domain(df, 50)
        
        counter = 0
        accepted_counter = 0
        
        if len(df) != 0:
            for indx in df.index.get_level_values("RequestId"):
                row = df.xs(indx, axis=0, level="RequestId").iloc[0]
                header = json.loads(row.loc["Header"])
                url = row.loc["URL"]
                
                accepted_status_codes = self._storage.get_accepted_status(indx)
                accepted = self._execute_web_request(indx, url, header, accepted_status_codes)
                
                if accepted:
                    accepted_counter += 1
                    
                counter += 1            
                # time.sleep(1.0)
            
        if counter != 0:
            self._logger.info("Pending: {:d}/{:d}".format(accepted_counter, counter))
            
        self._logger.info("Done PendingRequests")
            
        return counter != 0
    
    def fill_default_domain_timeouts (self):
        self._logger.info("Starting FillDomainTimeout.")
        unset_domain_ids = self._storage.get_domain_ids_without_domain_timeouts()
        timeouts = [
                self._timeout_default
                for _ in unset_domain_ids
            ]
        
        self._storage.direct_insert_domain_timeout(unset_domain_ids, timeouts)
        self._logger.info("Done FillDomainTimeout.")
        
        return len(timeouts) != 0
    
    def execute_failing_requests (self):
        self._logger.info("Start FailingRequests")
        df = self._storage.get_retryable_failing_request()
        df = self._split_fullrequest_dataframe_by_domain(df, 50)
        
        counter = 0
        accepted_counter = 0
        
        if len(df) != 0:
            for request_id in  df.index.get_level_values("RequestId"):
                row = df.xs(request_id, axis=0, level="RequestId").iloc[0]
                
                header = json.loads(row.loc["Header"])
                url = row.loc["URL"]
                
                accepted_status_codes = self._storage.get_accepted_status(request_id)
                accepted = self._execute_web_request(request_id, url, header, accepted_status_codes)
                
                if accepted:
                    accepted_counter += 1
                    
                counter += 1  
                # time.sleep(1.0)
            
        if counter != 0:
            self._logger.info("Failing: {:d}/{:d}".format(accepted_counter, counter))
            
        self._logger.info("Done FailingRequests")
            
        return counter != 0
    
    def execute_requests (self):
        self._logger.info("Start ExecuteRequests.")
        filled_timeouts = self.fill_default_domain_timeouts()
        executed_pending_requests = self._execute_pending_requests()
        executed_failing_requests = self.execute_failing_requests()
        
        msg = f"Execute requests: {filled_timeouts}, {executed_pending_requests}, {executed_failing_requests}"
        self._logger.info(msg)

        made_changes = filled_timeouts | executed_pending_requests | executed_failing_requests
        
        return made_changes
        
