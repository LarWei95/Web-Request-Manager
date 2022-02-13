'''
Created on 11.02.2022

@author: larsw
'''
from webrequestmanager.model.storage import Request, Response, Storage, URL, RequestHeader
import json
import datetime as dt

def get_dummy_data ():
    now_time = dt.datetime(2022, 2, 13, 15)
    
    request1 = Request(URL.of_string("http://www.test1.com/subpage1"),
                       RequestHeader.of_dict({}),
                       now_time,
                       200)
    response1_1 = Response(request1, 
                            200,
                            now_time + dt.timedelta(seconds=15),
                            "{}",
                            b'Hello1')
    
    request2 = Request(URL.of_string("http://www.test2.com/subpage2"),
                       RequestHeader.of_dict({}),
                       now_time + dt.timedelta(hours=1),
                       200)
    response2_1 = Response(request2, 
                            504, 
                            now_time + dt.timedelta(hours=1, minutes=5),
                            "{}",
                            b'')
    response2_2 = Response(request2, 
                            200, 
                            now_time + dt.timedelta(hours=1, minutes=10),
                            "{}",
                            b'Hello2')
    
    request3 = Request(URL.of_string("http://www.test3.com/subpage3"),
                       RequestHeader.of_dict({}),
                       now_time + dt.timedelta(hours=2),
                       [200, 301])
    response3_1 = Response(request3, 
                            504, 
                            now_time + dt.timedelta(hours=2, minutes=30),
                            "{}",
                            b'')
    response3_2 = Response(request3, 
                            301, 
                            now_time + dt.timedelta(hours=2, minutes=45),
                            "{}",
                            b'')
    
    request4 = Request(URL.of_string("http://www.test4.com/subpage4"),
                       RequestHeader.of_dict({}),
                       now_time + dt.timedelta(hours=4),
                       [200, 301])
    response4_1 = Response(request3, 
                            504, 
                            now_time + dt.timedelta(hours=4, minutes=45),
                            "{}",
                            b'')
    response4_2 = Response(request3, 
                            504, 
                            now_time + dt.timedelta(hours=5),
                            "{}",
                            b'')
    
    request5 = Request(URL.of_string("http://www.test5.com/subpage5"),
                       RequestHeader.of_dict({}),
                       now_time + dt.timedelta(hours=6),
                       200)
    
    request6 = Request(URL.of_string("http://www.test1.com/subpage1"),
                       RequestHeader.of_dict({"Accept" : "All"}),
                       now_time + dt.timedelta(hours=7),
                       200)
    response6_1 = Response(request6, 
                            501,
                            now_time + dt.timedelta(hours=7, minutes=10),
                            "{}",
                            b'')
    response6_2 = Response(request6, 
                            501,
                            now_time + dt.timedelta(hours=7, minutes=20),
                            "{}",
                            b'')
    response6_3 = Response(request6, 
                            501,
                            now_time + dt.timedelta(hours=7, minutes=30),
                            "{}",
                            b'')
    
    request7 = Request(URL.of_string("http://www.test2.com/subpage2"),
                       RequestHeader.of_dict({"Accept" : "All"}),
                       dt.datetime(2022, 2, 12, 20),
                       200)
    response7_1 = Response(request7, 
                            504, 
                            now_time + dt.timedelta(hours=8),
                            "{}",
                            b'')
    response7_2 = Response(request7, 
                            200, 
                            now_time + dt.timedelta(hours=8, seconds=15),
                            "{}",
                            b'Response was okay.')
    response7_3 = Response(request7, 
                            404, 
                            now_time + dt.timedelta(hours=8, seconds=30),
                            "{}",
                            b'')
    
    return {
            request1 : [response1_1],
            request2 : [response2_1, response2_2],
            request3 : [response3_1, response3_2],
            request4 : [response4_1, response4_2],
            request5 : [],
            request6 : [response6_1, response6_2, response6_3],
            request7 : [response7_1, response7_2, response7_3]
        }

def main ():
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "localhost"
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage(host, user, password)
    
    data_dict = get_dummy_data()
    
    for request in data_dict:
        responses = data_dict[request]
        url = request.url
        
        request_id = storage.insert_request(request)
        domain_id = storage.get_domain_id(url)
        storage.direct_insert_domain_timeout(domain_id, dt.timedelta(minutes=15))
        
        for response in responses:
            storage.direct_insert_response(request_id, response)

def testing ():
    with open("../../credentials.json", "r") as f:
        credentials = json.load(f)
    
    host = "localhost"
    user = credentials["user"]
    password = credentials["password"]
    
    storage = Storage(host, user, password)
    
    data_dict = get_dummy_data()
    
    for request in data_dict:
        request_id = storage.insert_request(request)
        print(request_id)
        
        print(storage.get_accepted_status(request_id))
        print(storage.get_latest_accepted_response(request_id))
        
    print(storage.get_requests_without_responses())
    print(storage.get_failing_request_timeouts())

if __name__ == '__main__':
    main()