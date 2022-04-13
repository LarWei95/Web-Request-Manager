'''
Created on 15.02.2022

@author: larsw
'''
import requests
import multiprocessing as mp
from bs4 import BeautifulSoup
import pandas as pd
from webrequestmanager.control.requester import HideMyNameProxyList, ProxyManager, Requester

def main ():
    header = {
            "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding" : "gzip, deflate, br",
            "Accept-Language" : "de,en-US;q=0.7,en;q=0.3",
            "Cache-Control" : "max-age=0",
            "Connection" : "keep-alive",
            "Sec-Fetch-Dest" : "document",
            "Sec-Fetch-Mode" : "navigate",
            "Sec-Fetch-Site" : "same-origin",
            "TE" : "trailers",
            "Upgrade-Insecure-Requests" : "1",
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0"
        }
    url = "http://www.hltv.org/"
    
    proxy_list = HideMyNameProxyList()
    proxy_manager = ProxyManager(proxy_list)
    
    requester = Requester(proxy_manager)
    
    r1 = requester.request(url, header, [200])
    print(r1)
    print("Main Proxy Data:\n"+str(proxy_manager.get_proxy_data()))
    
    proxy_manager.update(True)
    
    r2 = requester.request(url, header, [200])
    print(r2)
    print("Main Proxy Data:\n"+str(proxy_manager.get_proxy_data()))

if __name__ == '__main__':
    main()