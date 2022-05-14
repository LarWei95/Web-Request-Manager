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
from collections import defaultdict

class _StatusManager ():
    '''
    Manages the requestability of certain domains
    and used header ids.
    '''

    def __init__ (self, logger, bps_buffer_length=25):
        self._logger = logger

        self._bps_buffer_length = bps_buffer_length
        # UTC Timestamps
        self._bps_buffer_timestamps = defaultdict(list)
        self._bps_buffer_sizes = defaultdict(list)

        self._domain_status = None
        self._status_changed = None
        self._domain_policy_df = None
    
    def set_base_infos (self, domain_status, status_changed, 
                        domain_policy_df):
        self._logger.info(f"StatusManager: Setting base infos:\n{domain_status}\n{status_changed}\n{domain_policy_df}")

        self._domain_status = domain_status
        self._status_changed = status_changed
        self._domain_policy_df = domain_policy_df
    
    def put_bps_info (self, domain_id, timestamp, size):
        self._bps_buffer_timestamps[domain_id].append(timestamp)
        self._bps_buffer_sizes[domain_id].append(size)

        clen = len(self._bps_buffer_timestamps[domain_id])
        size_diff = self._bps_buffer_length - clen

        if size_diff < 0:
            new_timestamps = self._bps_buffer_timestamps[domain_id][-size_diff:]
            new_sizes = self._bps_buffer_sizes[domain_id][-size_diff:]

            self._bps_buffer_timestamps[domain_id] = new_timestamps
            self._bps_buffer_sizes[domain_id] = new_sizes

        self._logger.info(f"StatusManager: Putting BPSInfo {domain_id} {timestamp} {size}")
    
    def put_domain_status (self, domain_id, header_id, valid):
        key = (domain_id, header_id)

        new_status = 2 if valid else 1

        self._logger.info(f"StatusManager: Putting DS {new_status} for {key}")

        self._domain_status.at[key, "Status"] = new_status
        self._status_changed[key] = True

        t = self._domain_status
        self._logger.info(f"StatusManager: New Domain Status table:\n{t}")

    def get_domain_bps (self, domain_id):
        if len(self._bps_buffer_timestamps[domain_id]) == 0:
            bps = 0.0
        else:
            buf = self._bps_buffer_sizes[domain_id]
            start_timestamp = self._bps_buffer_timestamps[domain_id][0]
            utc_now = dt.datetime.utcnow()
            secs = (utc_now - start_timestamp).total_seconds()
            
            self._logger.info(f"DomainBPS: {start_timestamp} {utc_now} {secs} {buf}")
    
            bps = np.sum(buf) / secs

        return bps


    def _get_domain_mask (self):
        '''
        Returns a series with a domain id index
        and boolean values describing which
        domains can be requested at the moment.
        '''
        bps_limits = self._domain_policy_df["BPSLimit"]

        domain_ids = self._domain_policy_df.index.values

        bps_mask = [self.get_domain_bps(x) for x in domain_ids]
        bps_mask = [
                    True
                    if bps_limits[x] is None or np.isnan(bps_limits[x])
                    else bps < bps_limits[x]
                    for bps, x in zip(bps_mask, domain_ids)
                ]
        
        indx = pd.Index(domain_ids, name="DomainId")

        return pd.DataFrame({"DomainMask" : bps_mask}, index=indx)

    def _get_domain_header_mask (self):
        '''
        Returns a series with domain ids and header ids
        as index and boolean values describing which pairs
        of domain ids and header ids can be requested at the
        moment.
        '''
        # 1: Error, 2: OK
        statuses = self._domain_status["Status"]

        domain_header_ids = self._domain_status.index.values
        status_mask = [
                    statuses[domain_header_tuple] == 2
                    or self._status_changed[domain_header_tuple] == False
                    for domain_header_tuple in domain_header_ids
                ]

        indx = pd.MultiIndex.from_tuples(domain_header_ids, names=["DomainId", "HeaderId"])
        return pd.DataFrame({"DomainHeaderMask" : status_mask}, index=indx) 

    def pick_request (self, request_df):
        # DomainId -> DomainMask
        domain_mask = self._get_domain_mask()
        # DomainId, HeaderId -> DomainHeaderMask
        domain_header_mask = self._get_domain_header_mask()
        
        joined = domain_header_mask.join(domain_mask, on="DomainId")
        joined = (joined["DomainMask"] & joined["DomainHeaderMask"]).to_frame("Mask")
        joined = joined[joined["Mask"] == True]

        self._logger.info(f"StatusManager - Picking a request - Joined Options:\n{joined}")

        joined = request_df.join(joined, how="inner", on=["DomainId", "HeaderId"])

        self._logger.info(f"StatusManager - Picking a request - Final options:\n{joined}")

        joined = joined.index.get_level_values("RequestId").values
        
        self._logger.info(f"StatusManager - Picking a request - Final IDs:\n{joined}")

        candidates_count = len(joined)

        if candidates_count != 0:
            randint = np.random.randint(0, candidates_count)
            return joined[randint]
        else:
            return None

class RequestOrchestrator ():
    def __init__ (self, storage, requester, logger,
                  bps_buffer_length=25):
        self._storage = storage
        self._requester = requester
        
        self._logger = logger
        self._manager = _StatusManager(self._logger,
                                       bps_buffer_length=bps_buffer_length)

    def _split_request_df (self, request_df):
        splitted = {}

        unique_domains = request_df.index.get_level_values("DomainId").unique()

        for domain_id in unique_domains:
            selected = request_df.xs(domain_id, level="DomainId")

            splitted[domain_id] = selected

        return splitted
    
    def _random_delay (self, mindelay, maxdelay):
        delay_span = maxdelay - mindelay
        random_delay = np.random.rand() * delay_span + mindelay
        time.sleep(random_delay)


    def _try_get_size_of_response (self, response):
        if response is None:
            return 0.0
        else:
            return len(response.content)
    
    def _request_retry (self, single_request_df, accepted_status_codes,
                        policy, bytecounter):
        timeout = policy["Timeout"]
        force_proxy = bool(policy["ProxyDefault"])
        url = single_request_df["URL"].iloc[0]
        header = json.loads(single_request_df["Header"].iloc[0])
        
        is_https = "https:" in url.lower()
        url_http = url.replace("https:", "http:")
        

        retries = policy["Retries"]
        retry_mindelay = policy["RetryMinDelay"]
        retry_maxdelay = policy["RetryMaxDelay"]
        retry_http = bool(policy["RetryHTTP"]) and is_https
        # retry_proxies = bool(policy["RetryProxies"]) and not force_proxy

        self._logger.info(f"RequestOrchestrator: Reattempt routine started with {retries} - {url} - {url_http}")
        
        for _ in range(retries):
            self._random_delay(retry_mindelay, retry_maxdelay)
            response, _, valid = self._requester.request(
                                        url, header, accepted_status_codes,
                                        timeout, force_proxy
                                    )
            bytecounter += self._try_get_size_of_response(response)
            
            self._logger.info(f"RequestOrchestrator: basic reattempt yielded {response} {valid} with {bytecounter}")

            if valid:
                return response, bytecounter, valid
            
            # Additional policy attempts
            # HTTP attempt
            if retry_http:
                alt_response, _, valid = self._requester.request(
                                        url_http, header, accepted_status_codes,
                                        timeout, force_proxy
                                    )
                bytecounter += self._try_get_size_of_response(alt_response)
                
                self._logger.info(f"RequestOrchestrator: HTTP reattempt yielded {alt_response} {valid} with {bytecounter}")

                if valid:
                    return alt_response, bytecounter, valid

        return response, bytecounter, valid


    def _request (self, single_request_df, accepted_status_codes, policy):
        '''
        Gets a DataFrame with a single request and a
        policy series. Returns response information.
        '''
        bytecounter = 0

        timeout = policy["Timeout"]
        force_proxy = bool(policy["ProxyDefault"])
        url = single_request_df["URL"].iloc[0]
        header = json.loads(single_request_df["Header"].iloc[0])
        self._logger.info(f"RequestOrchestrator: General request of {url}\nwith Timeout={timeout}, ForceProxy={force_proxy}")
        response, _, valid = self._requester.request(
                    url, header, accepted_status_codes,
                    timeout, force_proxy
                )
        
        self._logger.info(f"RequestOrchestrator: Received: {response}, {valid}")
        
        bytecounter += self._try_get_size_of_response(response)

        if not valid:
           response, bytecounter, valid = self._request_retry(single_request_df,
                                                              accepted_status_codes,
                                                              policy, bytecounter)

        return response, bytecounter, valid
                    
    def _store_response(self, request_id, response, timestamp):
        if response is not None:
            response = Response.of_response(None, response, timestamp)
            self._storage.direct_insert_response(request_id, response)        

    def orchestrate (self, request_df):
        # request_df:
        #   Index:      RequestId, UrlId, DomainId, HeaderId
        #   Content:    Scheme, Netloc, Path, Query, 
        #               Header, Timestamp, URL, CompletingResponseId,
        #               ResponseCount
        #   Only new or retryable requests in the dataframe
        #
        # domain_policy_df:
        #   Index:      DomainId
        #   Content:    Timeout, Retries, RetryMinDelay, RetryMaxDelay,
        #               RetryHTTP, RetryProxies, BPSLimit, ProxyDefault,
        #               ProxyRegions
        
        # domain_status:
        #   Index:      DomainId, HeaderId
        #   Content:    Scheme, Netloc, Header, Timestamp, Status
        #   Status:     1: Error
        #               2: OK
        domain_status = self._storage.get_domain_status()
        status_changed = {
                (domain_id, header_id) : False
                for domain_id, header_id in domain_status.index.values
            }
        domain_policy_df = self._storage.get_domain_policy()
        self._manager.set_base_infos(domain_status, status_changed,
                                     domain_policy_df)
            
        # while has something:
        #   check which requests can be requested
        #   pick one
        #   request
        #   store result
        #   update which requests can be requested
        
        success_count = 0

        while True:
            request_id = self._manager.pick_request(request_df)
            
            if request_id is None:
                break
 
            accepted_status_codes = self._storage.get_accepted_status(request_id)
            
            self._logger.info(f"RequestOrchestrator: Picking request: {request_id} - {accepted_status_codes}")

            selected_request = request_df.xs(request_id, level="RequestId",
                                             drop_level=False)
            domain_id = selected_request.index.get_level_values("DomainId")[0]
            header_id = selected_request.index.get_level_values("HeaderId")[0]
            selected_policy = domain_policy_df.loc[domain_id]

            response, bytecount, valid = self._request(selected_request,
                                                       accepted_status_codes,
                                                       selected_policy)
            done_time = dt.datetime.utcnow()
            
            self._manager.put_bps_info(domain_id, done_time, bytecount)
            self._manager.put_domain_status(domain_id, header_id, valid)
            self._store_response(request_id, response, done_time)
            
            if valid:
                success_count += 1

            request_df = request_df.drop(request_id, level="RequestId")

            if len(request_df) == 0:
                break

        return success_count

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
        self._orchestrator = RequestOrchestrator(self._storage, self._requester,
                                                  self._logger)
        
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
       
        original_length = len(df)
        
        if original_length != 0:
            success_count = self._orchestrator.orchestrate(df)
        else:
            success_count = 0
        
        self._logger.info(f"PendingRequests: {success_count}/{original_length}")

        return original_length != 0
        ''' 

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
        '''
    
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
        
        original_length = len(df)
        
        if original_length != 0:
            success_count = self._orchestrator.orchestrate(df)
        else:
            success_count = 0
        
        self._logger.info(f"FailingRequests: {success_count}/{original_length}")

        return original_length != 0

        '''
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
        '''
    
    def execute_requests (self):
        self._logger.info("Start ExecuteRequests.")
        filled_timeouts = self.fill_default_domain_timeouts()
        # DomainId -> Timeout, Retries, RetryMinDelay, RetryMaxDelay,
        #               RetryHTTP, RetryProxies, BPSLimit,
        #               ProxyDefault, ProxyRegions
        policies = self._storage.get_domain_policy()

        executed_pending_requests = self._execute_pending_requests()
        executed_failing_requests = self.execute_failing_requests()
        
        msg = f"Execute requests: {filled_timeouts}, {executed_pending_requests}, {executed_failing_requests}"
        self._logger.info(msg)

        made_changes = filled_timeouts | executed_pending_requests | executed_failing_requests
        
        return made_changes
       
    def execute_maintenance(self):
        self._storage.fill_missing_request_statuses()
