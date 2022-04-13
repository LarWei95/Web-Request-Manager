'''
Created on 11.02.2022

@author: larsw
'''
from flask import Flask, request, jsonify, Response
import numpy as np
import multiprocessing as mp
from webrequestmanager.control.api import WebRequestAPIClient
import time
import traceback as tb

class TestServer ():
    def __init__ (self, name, port):
        self.name = name
        self.port = port
        
        self.app = Flask(self.name)
        
        @self.app.route("/")
        def primary ():
            first_param = request.args.get("number", type=int)
            
            random_first = np.random.randint(0, 30)
            
            first_diff = first_param - random_first
            
            if first_diff < 0:
                content = "Criteria not met."
                status_code = 501
            else:
                content = "Criteria met."
                status_code = 200
                
            return Response(content, status=status_code)
        
    def run (self):
        self.app.run(port=self.port)

def run_server_main (name, port):
    try:
        server = TestServer(name, port)
        server.run()
    except:
        tb.print_exc()

def run_client_main (ports):
    client = WebRequestAPIClient("http://127.0.0.1", 5000)
    url_base = "http://127.0.0.1:{:d}"
    print("Started client main.")
    
    urls = []
    
    for port in ports:
        url = url_base.format(port)
        
        suburls = [
            url+"/?number={:d}".format(i)
            for i in range(0, 30)
        ]
        urls.extend(suburls)
    
    print("URLs: "+str(urls))
    
    try:
        request_ids = [
                client.post_page_request(url, {}, min_date=None, max_date=None)
                for url in urls
            ]
        print("Request IDs: "+str(request_ids))
        for request_id in request_ids:
            response = client.get_response(request_id=request_id)
            print(response)
            
        print("GOT EVERYTHING!")
    except:
        tb.print_exc()
    
def main ():
    pool = mp.Pool()
    ports = list(range(18140, 18148))
    
    rs = [
            pool.apply_async(run_server_main, tuple([__name__, port]))
            for port in ports
        ]
        
    time.sleep(5)
    
    rm = pool.apply_async(run_client_main, tuple([ports]))
    
    for r in rs:
        r.get()
        
    rm.get()
    
if __name__ == '__main__':
    main()