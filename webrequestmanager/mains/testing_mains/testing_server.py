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

def get_app ():
    app = Flask(__name__)
    
    @app.route("/")
    def primary ():
        first_param = request.args.get("number", type=int)
        
        random_first = np.random.randint(0, 10)
        
        first_diff = first_param - random_first
        
        if first_diff < 0:
            content = "Criteria not met."
            status_code = 501
        else:
            content = "Criteria met."
            status_code = 200
            
        return Response(content, status=status_code)
    
    return app

def run_server_main ():
    app = get_app()
    print("Started server main.")
    app.run(port=18141)

def run_client_main ():
    client = WebRequestAPIClient("http://127.0.0.1", 5000)
    url_base = "http://127.0.0.1:18141"
    print("Started client main.")
    urls = [
            url_base+"/?number={:d}".format(i)
            for i in range(0, 20)
        ]
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
    
if __name__ == '__main__':
    POOL = mp.Pool()
    R1 = POOL.apply_async(run_server_main, tuple([]))
    
    time.sleep(5)
    R2 = POOL.apply_async(run_client_main, tuple([]))
    
    R1.get()
    R2.get()