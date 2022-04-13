'''
Created on 01.02.2022

@author: larsw
'''
from webrequestmanager.control.requesthandling import RequestHandler
from flask import Flask, request, jsonify
from pprint import pprint
import json
import gzip
from io import BytesIO
import requests
import datetime as dt
import pandas as pd
import time
import traceback as tb

URL_KEY = "url"
HEADER_KEY = "header"
MIN_DATE_KEY = "min_date"
MAX_DATE_KEY = "max_date"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

REQUESTID_KEY = "request_id"
STATUSCODE_KEY = "status_code"

def stringify_status_codes (status_code):
    if isinstance(status_code, int):
        status_code = str(status_code)
    else: 
        status_code = ",".join(str(x) for x in status_code)
        
    return status_code
    
def destringify_status_codes (status_code):
    if "," in status_code:
        status_code = [
                int(x.strip())
                for x in status_code.split(",")
            ]
    else:
        status_code = int(status_code.strip())
        
    return status_code

class WebRequestAPIServer ():
    # ? = %3F
    # / = %2F
    
    def __init__ (self, storage, requester):
        self._storage = storage
        self._handler = RequestHandler(requester, self._storage)
        
        self._app = Flask(__name__)
        self._register_callbacks(self._app)
        
    @classmethod
    def _conditional_to_datetime (cls, d, k):
        v = d.get(k, None)
        
        if v is not None:
            v = dt.datetime.strptime(v, DATETIME_FORMAT)
            
        return v
        
    @classmethod
    def decode_post_request (cls, post_request):
        header = bytes.fromhex(post_request[HEADER_KEY]).decode("utf-8")
        post_request[HEADER_KEY] = json.loads(header)
            
        url = bytes.fromhex(post_request[URL_KEY]).decode("utf-8")
        post_request[URL_KEY] = url
        
        status_codes = post_request.get(STATUSCODE_KEY, "200")
        status_codes = destringify_status_codes(status_codes)
        post_request[STATUSCODE_KEY] = status_codes
        
        post_request[MIN_DATE_KEY] = cls._conditional_to_datetime(post_request, MIN_DATE_KEY)
        post_request[MAX_DATE_KEY] = cls._conditional_to_datetime(post_request, MAX_DATE_KEY)
        
        return post_request
        
    def _process_status_code (self, status_code):
        if "," not in status_code:
            return int(status_code)
        else:
            status_code = status_code.split(",")
        
    def _register_callbacks (self, app):
        @app.route("/", methods=["POST", "GET"])
        def get_site ():
            if request.method == "POST":
                post_request = dict(request.form)
                post_request = WebRequestAPIServer.decode_post_request(post_request)
                
                request_id = self._handler.add_request(post_request[URL_KEY], 
                                          post_request[HEADER_KEY],
                                          post_request[STATUSCODE_KEY],
                                          post_request[MIN_DATE_KEY], 
                                          post_request[MAX_DATE_KEY])
                return jsonify({REQUESTID_KEY : request_id})
            else:
                request_id = int(request.args.get(REQUESTID_KEY))
                
                response = self._handler.get_response(request_id=request_id)
                
                if response is not None:
                    response = response.to_dict()
                    response["Content"] = response["Content"].hex()
                    return jsonify(response)
                else:
                    return jsonify({})
        
    def run (self, host=None, port=None):
        self._app.run(host=host, port=port)
        
class WebRequestAPIClient ():
    def __init__ (self, host, port):
        self._host = host
        self._port = port
        self._url = "{:s}:{:d}".format(self._host, self._port)
        
    @classmethod
    def prepare_page_request_params (cls, url, header, accepted_status, min_date, max_date):
        header = json.dumps(header).encode("utf-8").hex()
        url = url.encode("utf-8").hex()
        
        params = {
                URL_KEY : url,
                HEADER_KEY : header,
                STATUSCODE_KEY : stringify_status_codes(accepted_status)
            }
        
        if min_date is not None:
            min_date = min_date.strftime(DATETIME_FORMAT)
            params[MIN_DATE_KEY] = min_date
            
        if max_date is not None:
            max_date = max_date.strftime(DATETIME_FORMAT)
            params[MAX_DATE_KEY] = max_date
        
        return params
        
    def post_page_request (self, url, header, accepted_status=200, min_date=None, max_date=None):
        params = WebRequestAPIClient.prepare_page_request_params(url, header, accepted_status, min_date, max_date)
        r = requests.post(self._url, data=params)
        
        try:
            request_id = json.loads(r.content.decode("utf-8"))[REQUESTID_KEY]
        except Exception as e:
            print(e)
            pprint(r)
            tb.print_exc()
            raise e
        
        return request_id
    
    def _process_status_code (self, status_code):
        if isinstance(status_code, int):
            return str(status_code)
        else:
            status_code = [str(x) for x in status_code]
            status_code = ",".join(status_code)
            return status_code
    
    def get_response (self, url=None, header={}, min_date=None, max_date=None, request_id=None, wait=True):
        if url is None and request_id is None:
            errmsg = "Both URL and request id are None."
            raise ValueError(errmsg)
        
        if request_id is None:
            request_id = self.post_page_request(url=url, header=header, min_date=min_date, max_date=max_date)
        
        params = {
            REQUESTID_KEY : request_id
            }
        
        while True:
            r = requests.get(self._url, params=params)
            response = json.loads(r.content.decode("utf-8"))
            
            if "Header" in response and "Content" in response:
                response["Header"] = json.loads(response["Header"])
                response["Content"] = BytesIO(bytes.fromhex(response["Content"]))
                
                with gzip.open(response["Content"], "rb") as f:
                    response["Content"] = f.read()
                    
                response = pd.Series(response)
                    
                return response
            else:
                if not wait:
                    return None
            
            time.sleep(1)