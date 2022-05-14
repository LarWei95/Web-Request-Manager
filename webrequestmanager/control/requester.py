'''
Created on 15.02.2022

@author: larsw
'''
from abc import ABC, abstractmethod
import pandas as pd
import datetime as dt
import traceback as tb
import requests
from bs4 import BeautifulSoup
import numpy as np
from pprint import pprint

class ProxyList (ABC):
    TYPE_INDEX = "Type"
    IP_COLUMN = "IP"
    PORT_COLUMN = "Port"    
    
    def __init__ (self):
        self._proxy_df = ProxyList.create_empty_proxy_dataframe()
        self._last_update = None
    
    @classmethod
    def create_empty_proxy_dataframe (cls):
        indx = pd.Index(data=[],
                        name=cls.TYPE_INDEX)
        
        df = pd.DataFrame({
                cls.IP_COLUMN : [],
                cls.PORT_COLUMN : []
            }, index=indx)
        return df
    
    @abstractmethod
    def _create_updated_proxy_dataframe (self):
        pass
    
    def update_proxy_dataframe (self):
        new_df = self._create_updated_proxy_dataframe()
        updated = dt.datetime.now()
        
        self._proxy_df = new_df
        self._last_update = updated
    
    def get_proxy_dataframe (self):
        return self._proxy_df
    
    def get_last_update (self):
        return self._last_update
    
    def time_elapsed_since_update (self):
        if self._last_update is not None:
            return dt.datetime.now() - self._last_update
        else:
            return None
    
class HideMyNameProxyList (ProxyList):
    URL_FORMAT = "https://hidemy.name/de/proxy-list/?country=DE&type={:s}"
    FULL_URL_FORMAT = "https://hidemy.name/de/proxy-list/?country=DE&type=hs"
    HTTP_SYMBOL = "h"
    HTTPS_SYMBOL = "s"
    
    HEADER = {
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
    
    def __init__ (self):
        super().__init__()
        
    @classmethod
    def _scrape_list (cls, content):
        content = content.find("div", {"class" : "services_proxylist"})
        
        if content is not None:
            content = content.find("div", {"class" : "inner"})
            content = content.find("div", {"class" : "table_block"})
            content = content.find("table").find("tbody")
            
            rows = content.find_all("tr")
            
            types = []
            ips = []
            ports = []
            
            for row in rows:
                row = row.find_all("td")
                
                ip = row[0].get_text().strip()
                port = row[1].get_text().strip()
                connection_type = row[4].get_text().strip().lower()
                
                if "," not in connection_type:                    
                    types.append(connection_type)
                    ips.append(ip)
                    ports.append(port)
                else:
                    connection_type = connection_type.split(",")
                    
                    for ctype in connection_type:
                        ctype = ctype.strip().lower()
                        
                        types.append(ctype)
                        ips.append(ip)
                        ports.append(port)
                
            indx = pd.Index(types, name=HideMyNameProxyList.TYPE_INDEX)
            proxy_list = pd.DataFrame({
                    HideMyNameProxyList.IP_COLUMN : ips,
                    HideMyNameProxyList.PORT_COLUMN : ports
                }, index=indx)
            return proxy_list
        else:
            return cls.create_empty_proxy_dataframe()
        
    @classmethod
    def _request_url (cls, url):
        r = requests.get(url, headers=HideMyNameProxyList.HEADER)
        
        if r.status_code == 200:
            content = BeautifulSoup(r.content, "html.parser")
            
            if content is not None:
                return cls._scrape_list(content)
        
        return HideMyNameProxyList.create_empty_proxy_dataframe()
        
    def _create_updated_proxy_dataframe (self):        
        start = 0
        step = 64
        
        dfs = []
        
        while True:
            url = HideMyNameProxyList.FULL_URL_FORMAT+("&start={:d}".format(start))
            df = HideMyNameProxyList._request_url(url)
            
            if len(df) != 0:
                dfs.append(df)
                start += step
            else:
                break
            
        dfs = pd.concat(dfs, axis=0)
        
        return dfs
    
class MultiProxyList (ProxyList):
    def __init__ (self, proxy_lists):
        super().__init__()
        self._proxy_lists = proxy_lists
        
    def _create_updated_proxy_dataframe (self):
        all_updated = []
        
        for proxy_list in self._proxy_lists:
            try:
                proxy_list.update_proxy_dataframe()
            except:
                errmsg = """The update of proxy list {:s} caused an error at
                {:s}!""".format(str(proxy_list), str(dt.datetime.now()))
                print(errmsg)
                tb.print_exc()
                
            updated = proxy_list.get_proxy_dataframe()
            all_updated.append(updated)
            
                
class ProxyManager ():
    DELAY_COLUMN = "Delay"
    
    def __init__ (self, proxy_list, max_age=dt.timedelta(minutes=5)):
        self._proxy_list = proxy_list
        self._max_age = max_age
        
        self._proxy_data = None
        
    def _create_proxy_data (self):
        print("Called Create Proxy Data! "+str(self._proxy_data is None))
        proxy_df = self._proxy_list.get_proxy_dataframe().copy()
        
        proxy_df[ProxyManager.DELAY_COLUMN] = np.nan
        proxy_df = proxy_df.set_index([
                ProxyList.IP_COLUMN,
                ProxyList.PORT_COLUMN
            ], append=True)
        proxy_df = proxy_df.sort_index()
        proxy_df = proxy_df[~proxy_df.index.duplicated(keep='first')]
        
        
        if self._proxy_data is None:
            self._proxy_data = proxy_df
        else:
            old_data = self._proxy_data
            
            selector = ~old_data[ProxyManager.DELAY_COLUMN].isna().values
            selector = old_data[selector]
            selector = selector[~selector.index.duplicated(keep='first')]
            print("Selector:\n"+str(selector))
            
            proxy_df.update(selector)
            self._proxy_data = proxy_df
            
        
    def get_proxy_data (self):
        if self._proxy_data is None:
            self.update()
        
        return self._proxy_data
        
    def update (self, force=False):
        elapsed_time = self._proxy_list.time_elapsed_since_update()
        
        if elapsed_time is None:
            self._proxy_list.update_proxy_dataframe()
            self._create_proxy_data()
        elif (elapsed_time > self._max_age) or force:
            self._proxy_list.update_proxy_dataframe()
            self._create_proxy_data()
            
    def set_proxy_data_delay (self, protocol, ip, port, delay):
        self._proxy_data.loc[(protocol, ip, port)] = delay
        
        
class RequestModule (ABC):
    def __init__ (self):
        self.reinitialize_session()
    
    @abstractmethod
    def reinitialize_session (self):
        pass
        
    @abstractmethod
    def request (self, url, header, accepted_status_codes, use_cookies):
        pass
    
    # Reinitialize cookies after failure and try again
    

class DefaultRequestMethod (RequestModule):
    def __init__ (self):
        super().__init__()
        
    def reinitialize_session(self):
        self._session = requests.Session()
        self._cookies = None
    
    def request(self, url, header, accepted_status_codes, use_cookies):
        # Returns: Result of requests.get(...) / requests.Session().get(...)
        allow_redirects = 301 not in accepted_status_codes
        
        try:
            if use_cookies:
                cookies = self._cookies
            else:
                cookies = None
            
            # r2 = s.get(..., cookies=r1.cookies)
            d = dt.datetime.now()
            r = requests.get(url, headers=header, 
                             allow_redirects=allow_redirects,
                             cookies=cookies)
            self._cookies = r.cookies                
            d = (dt.datetime.now() - d).total_seconds() * 1000
            
            return r, d
        except:
            # tb.print_exc()
            return None, None
    
class Requester ():
    def __init__ (self, proxy_manager=None):
        self._proxy_manager = proxy_manager
        self._session = requests.Session()
        self._cookies = None
    
    def _prepare_accepted_status_codes (self, accepted_status_codes):
        if isinstance(accepted_status_codes, (list, tuple)):
            accepted_status_codes = set(accepted_status_codes)
        else:
            accepted_status_codes = set([accepted_status_codes])

        return accepted_status_codes


    def _response_valid (self, response, accepted_status_codes):
        if response is None:
            return False
        else:
            return response.status_code in accepted_status_codes
        
    def _request (self, url, header, allow_redirects, timeout, proxy_dict=None):
        try:
            # r2 = s.get(..., cookies=r1.cookies)
            d = dt.datetime.utcnow()

            for _ in range(3):
                try:
                    r = requests.get(url, headers=header, 
                              proxies=proxy_dict, allow_redirects=allow_redirects,
                              timeout=timeout)
                except requests.exceptions.ConnectionError as e:
                    r = None
                except requests.exceptions.ReadTimeout as e:
                    r = None
                except Exception as e:
                    raise e
                
            if r is None:
                d = None
            else:
                # self._cookies = r.cookies                
                d = (dt.datetime.utcnow() - d).total_seconds() * 1000
            
            return r, d
        except:
            tb.print_exc()
            return None, None
    
    def _proxy_request (self, url, header, accepted_status_codes, timeout):
        # Returns: Result of requests.get(...) / requests.Session().get(...)
        allow_redirects = 301 not in accepted_status_codes
        url = url.replace("https:", "http:")
        required_protocol = url.split(":")[0]
        
        sorted_data = self._proxy_manager.get_proxy_data().sort_values(by=ProxyManager.DELAY_COLUMN)
        available_protocols = sorted_data.index.get_level_values(ProxyList.TYPE_INDEX).unique()
        sorted_data = sorted_data.xs(required_protocol, level=ProxyList.TYPE_INDEX)
        
        for indx_value in sorted_data.index:
            proxy_url = "{:s}://{:s}:{:s}".format(required_protocol, indx_value[0], indx_value[1])
            proxy_dict = {required_protocol : proxy_url}
            
            response, duration = self._request(url, header, allow_redirects, timeout,
                                               proxy_dict=proxy_dict)
            
            if response is None:
                self._proxy_manager.set_proxy_data_delay(required_protocol, 
                                                         indx_value[0], 
                                                         indx_value[1],
                                                         np.nan)
                continue
            else:
                self._proxy_manager.set_proxy_data_delay(required_protocol, 
                                                         indx_value[0], 
                                                         indx_value[1],
                                                         duration)
                
            if response.status_code in accepted_status_codes:
                return response, duration
            
        return None, None
            
    def _direct_request (self, url, header, accepted_status_codes, timeout):
        # Returns: Result of requests.get(...) / requests.Session().get(...)
        allow_redirects = 301 not in accepted_status_codes
        
        response, secs = self._request(url, header, allow_redirects, timeout, 
                                    proxy_dict=None)
                
        return response, secs
            
    def old_request (self, url, header, accepted_status_codes):
        if isinstance(accepted_status_codes, (list, tuple)):
            accepted_status_codes = set(accepted_status_codes)
        else:
            accepted_status_codes = set([accepted_status_codes])
            
        main_response = self._direct_request(url, header, accepted_status_codes)
        main_valid = self._response_valid(main_response, accepted_status_codes)
            
        if main_valid:
            return main_response
        elif self._proxy_manager is not None:
            secondary_response = self._proxy_request(url, header, accepted_status_codes)
            secondary_valid = self._response_valid(secondary_response, accepted_status_codes)
            
            if secondary_valid:
                return secondary_response
            else:
                self._proxy_manager.update(force=True)
                
                tertiary_response = self._proxy_request(url, header, accepted_status_codes)
                tertiary_valid = self._response_valid(secondary_response, accepted_status_codes)
                
                if tertiary_valid:
                    return tertiary_response
                else:
                    return main_response
                
        return main_response
       

    def request (self, url, header, accepted_status_codes,
                        timeout, force_proxy):
        accepted_status_codes = self._prepare_accepted_status_codes(accepted_status_codes)

        if force_proxy:
            if self._proxy_manager is None:
                errmsg = "Proxy usage is forced by default, but no manager is given."
                raise ValueError(errmsg)
            
            response, secs = self._proxy_request(url, header,
                                                 accepted_status_codes, timeout)
        else:
            response, secs = self._direct_request(url, header, 
                                                  accepted_status_codes, timeout)

        valid = self._response_valid(response, accepted_status_codes)

        return response, secs, valid
