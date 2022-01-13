'''
Created on 12.01.2022

@author: larsw
'''
import mysql.connector
from urllib.parse import urlparse
import hashlib
import numpy as np
import pandas as pd
import json
from io import BytesIO
import gzip
import re

class URL ():
    def __init__ (self, urlparsed, path_checksum, query_checksum):
        self.urlparsed = urlparsed
        self.path_checksum = path_checksum
        self.query_checksum = query_checksum
    
    @classmethod
    def of_string (cls, url):
        urlparsed = urlparse(url, allow_fragments=False)
        
        path_checksum = hashlib.md5(urlparsed.path.encode("utf-8")).digest()
        query_checksum = hashlib.md5(urlparsed.query.encode("utf-8")).digest()
        
        return URL(urlparsed, path_checksum, query_checksum)
    
class RequestHeader ():
    def __init__ (self, header_dict, stringed_header_dict, header_checksum):
        self.header_dict = header_dict
        self.stringed_header_dict = stringed_header_dict
        self.header_checksum = header_checksum
        
    @classmethod
    def of_dict (cls, d):
        stringed = json.dumps(d)
        checksum = hashlib.md5(stringed.encode("utf-8")).digest()
        
        return RequestHeader(d, stringed, checksum)
        
class Request ():
    def __init__ (self, url, request_header, timestamp):
        self.url = url
        self.request_header = request_header
        self.timestamp = timestamp
        
class Response ():
    def __init__ (self, request, status_code, headers, content):
        self.request = request
        self.status_code = status_code
        self.headers = headers
        self.content = content
        
    @classmethod
    def of_response (cls, request, requests_response):
        status_code = requests_response.status_code
        headers = json.dumps(dict(requests_response.headers))
        headers = headers.replace("\"", "\\\"")
        headers = headers.replace("\\\\\"", "\\\\\\\"")
        
        c = requests_response.content
        
        if c is not None:
            content = BytesIO()
            
            with gzip.open(content, "wb") as f:
                f.write(c)
                
            content = content.getbuffer()
        else:
            content = None
            
        return Response(request, status_code, headers, content)

class Storage ():
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    FULLREQUEST_COLUMNS = ["RequestId", "UrlId", "DomainId", "Scheme", "Netloc",
                           "Path", "Query", "Header", "Timestamp", "URL"]
    FULLREQUEST_INDEX = "RequestId"
    
    URLPARSE_REVERT = "{:s}://{:s}{:s}{:s}"
    
    def __init__ (self, host, user, passwd, db_name="webrequest"):
        self._con = mysql.connector.connect(
                host=host,
                user=user,
                password=passwd
            )
        self._cur = self._con.cursor()
        
        self.db_name = db_name
        
        self._initialize()
        
    def _create_database (self, db_name):
        sql = "CREATE DATABASE IF NOT EXISTS {:s};".format(
                db_name
            )
        self._cur.execute(sql)
        self._con.commit()
        
        self._cur.execute("USE {:s};".format(db_name))
        
    def _create_domain_table (self):
        sql = """CREATE TABLE IF NOT EXISTS domain (
            domainid INTEGER UNSIGNED AUTO_INCREMENT,
            scheme CHAR(16),
            netloc CHAR(128),
            PRIMARY KEY (domainid),
            CONSTRAINT scheme_netloc_pair UNIQUE (scheme, netloc)
        );"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_url_table (self):        
        sql = """CREATE TABLE IF NOT EXISTS url (
            urlid INTEGER UNSIGNED AUTO_INCREMENT,
            domainid INTEGER UNSIGNED NOT NULL,
            pathchecksum BINARY(16) NOT NULL,
            querychecksum BINARY(16) NOT NULL,
            path TEXT NOT NULL,
            query TEXT NOT NULL,
            
            PRIMARY KEY (urlid),
            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
            CONSTRAINT url_checksum_pair UNIQUE (domainid, pathchecksum, querychecksum)
        );"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_request_header_table (self):
        sql = """CREATE TABLE IF NOT EXISTS requestheader (
            headerid INTEGER UNSIGNED AUTO_INCREMENT,
            headerchecksum BINARY(16) NOT NULL,
            header TEXT NOT NULL,
            
            PRIMARY KEY (headerid),
            CONSTRAINT unique_headerchecksum UNIQUE (headerchecksum)
        );"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_request_table (self):
        sql = """CREATE TABLE IF NOT EXISTS request (
            requestid INTEGER UNSIGNED AUTO_INCREMENT,
            urlid INTEGER UNSIGNED NOT NULL,
            headerid INTEGER UNSIGNED NOT NULL,
            date DATETIME NOT NULL,
            
            PRIMARY KEY (requestid),
            FOREIGN KEY (urlid)
                REFERENCES url(urlid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
            FOREIGN KEY (headerid)
                REFERENCES requestheader(headerid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
            CONSTRAINT unique_request UNIQUE (urlid, headerid, date)
        );"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_response_table (self):
        sql = """CREATE TABLE IF NOT EXISTS response (
            responseid INTEGER UNSIGNED AUTO_INCREMENT,
            requestid INTEGER UNSIGNED NOT NULL,
            requested DATETIME NOT NULL,
            statuscode SMALLINT UNSIGNED NOT NULL,
            header TEXT NOT NULL,
            content LONGBLOB NULL,
            
            PRIMARY KEY (responseid),
            FOREIGN KEY (requestid)
                REFERENCES request(requestid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_full_request_view (self):
        sql = """CREATE VIEW IF NOT EXISTS fullrequest AS 
        SELECT 
                req.requestid, req.urlid, 
                url.domainid, d.scheme, 
                d.netloc, 
                url.path, url.query, 
                reqh.header, req.date,
                CONCAT(d.scheme, 
                    "://", d.netloc, 
                    url.path, 
                    IF(url.query != "", 
                        CONCAT("?", url.query), 
                        url.query)
                ) "url"
        FROM request AS req 
        INNER JOIN url 
            ON req.urlid = url.urlid 
        INNER JOIN domain AS d 
            ON url.domainid = d.domainid 
        INNER JOIN requestheader AS reqh 
            ON req.headerid = reqh.headerid"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _create_domain_status_view (self):
        sql = """CREATE VIEW IF NOT EXISTS domainstatus AS
        SELECT 
            d.domainid, d.scheme, d.netloc,
            MAX(resp.requested) "requested",
            resp.statuscode
        FROM response AS resp
        INNER JOIN request AS req
            ON resp.requestid = req.requestid
        INNER JOIN url
            ON req.urlid = url.urlid
        INNER JOIN domain AS d
            ON url.domainid = d.domainid
        GROUP BY d.domainid;"""
        self._cur.execute(sql)
        self._con.commit()
        
    def _initialize (self):
        self._create_database(self.db_name)
        
        self._create_domain_table()
        self._create_url_table()
        
        self._create_request_header_table()
        self._create_request_table()
        self._create_response_table()
        
        self._create_full_request_view()
        self._create_domain_status_view()
        
    def get_last_insert_id (self):
        sql = "SELECT LAST_INSERT_ID();"
        self._cur.execute(sql)
        
        row = self._cur.fetchall()[0][0]
        return row
        
    def direct_insert_domain (self, url):
        sql = "INSERT INTO domain (scheme, netloc) VALUES {:s};"
        
        fmt = "(\"{:s}\",\"{:s}\")" 
        
        if isinstance(url, URL):
            url = fmt.format(
                        url.urlparsed.scheme,
                        url.urlparsed.netloc
                    )
            
            sql = sql.format(url)
            
            self._cur.execute(sql)
            self._con.commit()
            
            last_id = self.get_last_insert_id()
        else:
            last_id = []
            
            for x in url:
                last_id.append(self.direct_insert_domain(x))
                
            last_id = np.array(last_id)
    
        return last_id
    
    def get_domain_id (self, url):
        if isinstance(url, URL):
            sql = """SELECT domainid
            FROM domain
            WHERE scheme = \"{:s}\" AND
            netloc = \"{:s}\";""".format(
                    url.urlparsed.scheme,
                    url.urlparsed.netloc
                )
            self._cur.execute(sql)
            
            rows = self._cur.fetchall()
            
            if len(rows) == 0:
                return None
            else:
                return rows[0][0]
        else:
            ids = np.array([
                    self.get_domain_id(x)
                    for x in url
                ])
            return ids
        
    def insert_domain (self, url):
        multi = not isinstance(url, URL)
        
        existing_ids = self.get_domain_id(url)
        
        if multi:
            count = len(url)
            
            addables = []            
            
            domain_ids = np.empty(count, dtype=np.int)
            domain_ids_set = np.full(count, False)
            
            for i in range(count):
                x = url[i]
                existing_id = existing_ids[i]
                
                if existing_id is None:
                    addables.append(x)
                else:
                    domain_ids[i] = existing_id
                    domain_ids_set[i] = True
                    
            new_domain_ids = self.direct_insert_domain(addables)
            
            unset_indices = np.where(domain_ids_set == False)[0]
            domain_ids[unset_indices] = new_domain_ids
            return domain_ids
        else:
            if existing_ids is None:
                return self.direct_insert_domain(url)
            else:
                return existing_ids
            
    def direct_insert_url (self, domain_id, url):
        sql = """INSERT INTO url (domainid, pathchecksum, querychecksum, path, query)
        VALUES {:s};"""
        
        fmt = "({:d},X\'{:s}\',X\'{:s}\',\"{:s}\",\"{:s}\")" 
        
        if isinstance(url, URL):
            url = fmt.format(
                        domain_id,
                        url.path_checksum.hex(),
                        url.query_checksum.hex(),
                        url.urlparsed.path,
                        url.urlparsed.query
                    )
            
            sql = sql.format(url)
            
            self._cur.execute(sql)
            self._con.commit()
            
            last_id = self.get_last_insert_id()
        else:
            last_id = []
            
            for did, x in zip(domain_id, url):
                last_id.append(self.direct_insert_url(did, x))
                
            last_id = np.array(last_id)
            
        return last_id
    
    def get_url_id (self, url):
        domain_id = self.get_domain_id(url)
        
        if isinstance(url, URL):
            if domain_id is None:
                return None
            else:
                sql = """SELECT urlid
                FROM url
                WHERE domainid = {:d} AND
                pathchecksum = X'{:s}' AND
                querychecksum = X'{:s}';""".format(
                        domain_id,
                        url.path_checksum.hex(),
                        url.query_checksum.hex()
                    )
                self._cur.execute(sql)
                
                rows = self._cur.fetchall()
                
                if len(rows) == 0:
                    return None
                else:
                    return rows[0][0]
        else:
            ids = np.array([
                    self.get_url_id(x)
                    for x in url
                ])
            return ids
        
    def insert_url (self, url):
        multi = not isinstance(url, URL)
        
        domain_ids = self.insert_domain(url)        
        existing_ids = self.get_url_id(url)
        
        if multi:
            count = len(url)
            
            addables = []            
            
            url_ids = np.empty(count, dtype=np.int)
            url_ids_set = np.full(count, False)
            
            for i in range(count):
                x = url[i]
                existing_id = existing_ids[i]
                
                if existing_id is None:
                    addables.append(x)
                else:
                    url_ids[i] = existing_id
                    url_ids_set[i] = True
            
            new_url_ids = self.direct_insert_url(domain_ids[url_ids_set == False], addables)
            
            unset_indices = np.where(url_ids_set == False)[0]
            url_ids[unset_indices] = new_url_ids
            return url_ids
        else:
            if existing_ids is None:
                return self.direct_insert_url(domain_ids, url)
            else:
                return existing_ids
            
    def direct_insert_request_header (self, request_header):
        '''
        headerid INTEGER UNSIGNED AUTO_INCREMENT,
            headerchecksum BINARY(16) NOT NULL,
            header TEXT NOT NULL,
            
            PRIMARY KEY (headerid),
            CONSTRAINT unique_headerchecksum UNIQUE (headerchecksum)
        '''
        
        sql = """INSERT INTO requestheader (headerchecksum, header)
        VALUES {:s};"""
        
        fmt = "(X\'{:s}\',\"{:s}\")"
        
        if isinstance(request_header, RequestHeader):
            fmt = fmt.format(
                request_header.header_checksum.hex(), 
                request_header.stringed_header_dict.replace("\"", "\\\""))
            sql = sql.format(fmt)
            
            self._cur.execute(sql)
            self._con.commit()
            
            last_id = self.get_last_insert_id()
        else:
            last_id = []
            
            for x in request_header:
                last_id.append(self.direct_insert_request_header(x))
                
            last_id = np.array(last_id)
            
        return last_id
    
    def get_request_header_id (self, request_header):
        if isinstance(request_header, RequestHeader):
            sql = """SELECT headerid
            FROM requestheader
            WHERE headerchecksum = X'{:s}';          
            """.format(request_header.header_checksum.hex())
            
            self._cur.execute(sql)
                
            rows = self._cur.fetchall()
            
            if len(rows) == 0:
                return None
            else:
                return rows[0][0]
        else:
            ids = np.array([
                    self.get_request_header_id(x)
                    for x in request_header
                ])
            return ids
        
    def insert_request_header (self, request_header):
        multi = not isinstance(request_header, RequestHeader)
             
        existing_ids = self.get_request_header_id(request_header)
        
        if multi:
            count = len(request_header)
            
            addables = []            
            
            reqheader_ids = np.empty(count, dtype=np.int)
            reqheader_ids_set = np.full(count, False)
            
            for i in range(count):
                x = request_header[i]
                existing_id = existing_ids[i]
                
                if existing_id is None:
                    addables.append(x)
                else:
                    reqheader_ids[i] = existing_id
                    reqheader_ids_set[i] = True
            
            new_reqheader_ids = self.direct_insert_request_header(addables)
            
            unset_indices = np.where(reqheader_ids_set == False)[0]
            reqheader_ids[unset_indices] = new_reqheader_ids
            return reqheader_ids
        else:
            if existing_ids is None:
                return self.direct_insert_request_header(request_header)
            else:
                return existing_ids
            
    def direct_insert_request (self, url_id, header_id, timestamp):
        sql = "INSERT INTO request (urlid, headerid, date) VALUES {:s};"
        
        fmt = "({:d}, {:d}, \"{:s}\")"
        
        if not isinstance(url_id, (list, np.ndarray)):
            fmt = fmt.format(url_id, 
                             header_id, 
                             timestamp.strftime(Storage.DATETIME_FORMAT))
            sql = sql.format(fmt)
            
            self._cur.execute(sql)
            self._con.commit()
            
            last_id = self.get_last_insert_id()
        else:
            last_id = []
            
            for ui, hi in zip(url_id, header_id):
                last_id.append(self.direct_insert_request(ui, hi))
                
            last_id = np.array(last_id)
            
        return last_id
            
    def _get_request_date_comparison (self, exact_timestamp,
                                      min_timestamp, max_timestamp):
        if exact_timestamp is not None:
            sql = " AND date = \"{:s}\"".format(
                    exact_timestamp.strftime(Storage.DATETIME_FORMAT)
                )
        elif min_timestamp is not None or max_timestamp is not None:
            constraints = []
            
            if min_timestamp is not None:
                constraints.append("date >= \"{:s}\"".format(
                        min_timestamp.strftime(Storage.DATETIME_FORMAT)
                    ))
                
            if max_timestamp is not None:
                constraints.append("date < \"{:s}\"".format(
                        max_timestamp.strftime(Storage.DATETIME_FORMAT)
                    ))
            
            sql = " AND {:s}".format(
                    " AND ".join(constraints)
                )
        else:
            sql = ""
            
        return sql
            
    def get_request_id (self, url_id, header_id, exact_timestamp=None,
                        min_timestamp=None, max_timestamp=None):
        if not isinstance(url_id, (list, np.ndarray)):
            sql = """SELECT requestid FROM request
            WHERE 
            urlid = {:d} AND 
            headerid = {:d}{:s} ORDER BY date DESC;""".format(
                    url_id, header_id,
                    self._get_request_date_comparison(exact_timestamp, 
                                                      min_timestamp, 
                                                      max_timestamp)
                )
            
            self._cur.execute(sql)
                
            rows = self._cur.fetchall()
            
            if len(rows) == 0:
                return None
            else:
                return np.array([
                        x[0]
                        for x in rows
                    ])
        else:
            return np.array([
                    self.get_request_id(ui, hi)
                    for ui, hi in zip(url_id, header_id)
                ])
            
    def insert_request (self, request, min_date=None, max_date=None):
        multi = not isinstance(request, Request)
             
        if multi:
            ids = [
                    self.insert_request(x)
                    for x in request
                ]
            
            return ids
        else:
            url_id = self.insert_url(request.url)
            header_id = self.insert_request_header(request.request_header)
            
            if min_date is not None or max_date is not None:
                existing_id = self.get_request_id(url_id, header_id, 
                                                  min_timestamp=min_date, max_timestamp=max_date)                
            else:
                existing_id = self.get_request_id(url_id, header_id, request.timestamp)
            
            if existing_id is None:
                existing_id = self.direct_insert_request(url_id, header_id, request.timestamp)
            else:
                existing_id = existing_id[0]
                
            return existing_id
        
    def direct_insert_response (self, request_id, timestamp, response):
        content = response.content
        
        if isinstance(response, Response):
            if content is None:
                content = "NULL"
            else:
                content = "X'{:s}'".format(content.hex())
            
            sql = """INSERT INTO response 
            (requestid, requested, statuscode, header, content) 
            VALUES {:s};"""
            fmt = "({:d}, \"{:s}\", {:d}, \"{:s}\", {:s})"
            
            fmt = fmt.format(
                    request_id,
                    timestamp.strftime(Storage.DATETIME_FORMAT),
                    response.status_code,
                    response.headers,
                    content
                )
            sql = sql.format(fmt)
                
            self._cur.execute(sql)
            self._con.commit()
            
            last_id = self.get_last_insert_id()
        else:
            last_id = []
            
            for ri, tt, r in zip(request_id, timestamp, response):
                last_id.append(self.direct_insert_response(ri, tt, r))
                
            last_id = np.array(last_id)
            
        return last_id
    
    @classmethod
    def _get_iterable_condition (cls, attribute, values, is_string=False):
        if values is None:
            return None
        else:
            if is_string:
                element_format = "\"{:s}\""
            else:
                element_format = "{:s}"
            
            if isinstance(values, (list, set)):
                condition = ",".join(
                        element_format.format(str(x))
                        for x in values
                    )
                condition = "{:s} IN ({:s})".format(
                        attribute, condition
                    )
            else:
                condition = element_format.format(str(values))
                condition = "{:s} = {:s}".format(
                        attribute, condition
                    )
                
            return condition
        
    @classmethod
    def _get_range_condition (cls, attribute, values, is_string=False):
        if values is None:
            return None
        else:
            if is_string:
                element_format = "\"{:s}\""
            else:
                element_format = "{:s}"
                
            if isinstance(values, tuple):
                vmin, vmax = values
                
                condition = "{:s} BETWEEN {:s} AND {:s}".format(
                        attribute,
                        element_format.format(str(vmin)),
                        element_format.format(str(vmax))
                    )
            elif isinstance(values, (list, set)):
                subconditions = []
                subcondition_format = "({:s} BETWEEN {:s} AND {:s})"
                
                for x in values:
                    if isinstance(x, tuple):
                        xmin, xmax = x
                        
                        subcondition = subcondition_format.format(
                                attribute,
                                element_format.format(str(xmin)),
                                element_format.format(str(xmax))
                            )
                        subconditions.append(subcondition)
                    else:
                        errmsg = """The given range element in the range list
                        is not of type tuple. Given: {:s}
                        """.format(str(type(x)))
                        raise ValueError(errmsg)
                
                condition = "({:s})".format(" OR ".join(subconditions))
            else:
                condition = element_format.format(str(values))
                condition = "{:s} = {:s}".format(
                        attribute, condition
                    )
                
            return condition
        
    @classmethod
    def _get_datetime_range_condition (cls, attribute, values):
        if values is None:
            return None
        else:
            if isinstance(values, tuple):
                values = [
                        x.strftime(cls.DATETIME_FORMAT)
                        for x in values
                    ]
            elif isinstance(values, (list, set)):
                values = [
                        tuple([
                                x.strftime(cls.DATETIME_FORMAT)
                                for x in tup
                            ])
                        for tup in values
                    ]
            else:
                values = values.strftime(cls.DATETIME_FORMAT)
                
            return cls._get_range_condition(attribute, values, is_string=True)
        
    def _create_get_response_conditions (self, response_id, request_id, timestamp):
        conditions = []
        
        response_id = Storage._get_iterable_condition("responseid", response_id, is_string=False)
        request_id = Storage._get_iterable_condition("requestid", request_id, is_string=False)
        timestamp = Storage._get_datetime_range_condition("requested", timestamp)
        
        if response_id is not None:
            conditions.append(response_id)
            
        if request_id is not None:
            conditions.append(request_id)
            
        if timestamp is not None:
            conditions.append(timestamp)
            
        if len(conditions) != 0:
            conditions = " WHERE {:s}".format(
                    " AND ".join(conditions)
                )
        else:
            conditions = ""
            
        return conditions
        
    def get_response (self, response_id=None, request_id=None, timestamp=None):
        pass
    
    
    def get_latest_response (self, request_id, status_code=None):
        if status_code is None:
            sc_sql = ""
        else:
            sc_sql = " AND statuscode = {:d}".format(status_code)
        
        sql = """SELECT responseid, requested, statuscode, header, content
        FROM response
        WHERE requestid = {:d}{:s}
        ORDER BY requested DESC LIMIT 1;
        """.format(request_id, sc_sql)
        
        self._cur.execute(sql)
        rows = self._cur.fetchall()
        
        
        if len(rows) == 1:
            df = pd.Series(rows[0], index=["ResponseId", "Timestamp", "StatusCode", "Header", "Content"])
        else:
            df = None
        
        return df
    
    @classmethod
    def _prepare_fullrequest_dataframe (cls, rows):
        df = pd.DataFrame(rows, columns=Storage.FULLREQUEST_COLUMNS)
        df = df.set_index(Storage.FULLREQUEST_INDEX)
        '''
        full_urls = []
        
        for indx in df.index.values:
            row = df.loc[indx]
            
            scheme = row["Scheme"]
            netloc = row["Netloc"]
            path = row["Path"]
            query = row["Query"]
            
            if query != "":
                query = "?"+query
            
            url = cls.URLPARSE_REVERT.format(
                    scheme, netloc,
                    path, query
                )
            full_urls.append(url)
        
        df["URL"] = full_urls
        '''
        return df
        
    def get_requests_without_responses (self):
        sql = """SELECT
            fr.requestid, fr.urlid, fr.domainid,
            fr.scheme, fr.netloc, fr.path, fr.query,
            fr.header,
            fr.date, fr.url
        FROM fullrequest AS fr
        LEFT JOIN response AS resp
            ON fr.requestid = resp.requestid
        GROUP BY fr.requestid
        HAVING COUNT(resp.responseid) = 0;
        """
        
        self._cur.execute(sql)
        rows = self._cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df
    
    def get_requests_without_coded_responses (self, status_code=200):
        sql = """SELECT
            fr.requestid, fr.urlid, fr.domainid,
            fr.scheme, fr.netloc, fr.path, fr.query,
            fr.header,
            fr.date, fr.url
        FROM fullrequest AS fr
        LEFT JOIN response AS resp
            ON (fr.requestid = resp.requestid AND resp.statuscode = {:d})
        GROUP BY fr.requestid
        HAVING COUNT(resp.responseid) = 0;
        """.format(status_code)
        
        self._cur.execute(sql)
        rows = self._cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df