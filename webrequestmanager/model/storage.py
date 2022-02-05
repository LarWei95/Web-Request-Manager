'''
Created on 12.01.2022

@author: larsw
'''
import mysql.connector
from mysql.connector import RefreshOption
from urllib.parse import urlparse
import hashlib
import numpy as np
import pandas as pd
import json
from io import BytesIO
import gzip
import datetime as dt

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
    
class _DBCon ():
    def __init__ (self, host, user, passwd, db_name):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db_name = db_name
        
        self._con = None
        
    def __enter__ (self):
        self._con = mysql.connector.connect(
                host=self._host,
                user=self._user,
                password=self._passwd,
                database=self._db_name
            )
        return self._con.cursor()
    
    def __exit__ (self, exc_type, exc_val, exc_tb):
        self._con.commit()
        self._con.close()
    
class Storage ():
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    TIME_FORMAT = "%H:%M:%S"
    
    FULLREQUEST_QUERY = """SELECT 
                req.requestid, req.urlid, 
                url.domainid, req.headerid,
                d.scheme, d.netloc, 
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
        INNER JOIN request_header AS reqh 
            ON req.headerid = reqh.headerid"""
    FULLREQUEST_COLUMNS = ["RequestId", "UrlId", "DomainId", "HeaderId", "Scheme", "Netloc",
                           "Path", "Query", "Header", "Timestamp", "URL"]
    FULLREQUEST_INDEX = ["RequestId", "UrlId", "DomainId", "HeaderId"]
    
    DOMAINSTATUS_QUERY = """SELECT 
            d.domainid, reqh.headerid, 
            d.scheme, d.netloc, reqh.header,
            MAX(resp.requested) "requested",
            req.requestid,
            resp.statuscode
        FROM response AS resp
        INNER JOIN request AS req
            ON resp.requestid = req.requestid
        INNER JOIN request_header AS reqh
            ON req.headerid = reqh.headerid
        INNER JOIN url
            ON req.urlid = url.urlid
        INNER JOIN domain AS d
            ON url.domainid = d.domainid
        GROUP BY d.domainid, reqh.headerid
    """
    DOMAINSTATUS_COLUMNS = ["DomainId", "HeaderId", "Scheme", "Netloc", 
                            "Header", "Timestamp", "RequestId", "Statuscode"]
    DOMAINSTATUS_INDEX = ["DomainId", "HeaderId"]
    
    LATESTTIMEOUT_QUERY = """SELECT dt.domainid, dt.timeout_set, dt.timeout
        FROM domain_timeout AS dt
        INNER JOIN 
            (SELECT dtt.domainid, MAX(dtt.timeout_set) "timeout_set"
            FROM domain_timeout AS dtt
            GROUP BY dtt.domainid) temp
        ON dt.domainid = temp.domainid AND dt.timeout_set = temp.timeout_set"""
    DOMAINTIMEOUT_COLUMNS = ["DomainId", "TimeoutSet", "Timeout"]
    DOMAINTIMEOUT_INDEX = ["DomainId", "TimeoutSet"]
    
    RETRY_COLUMN = "Retry"
    
    URLPARSE_REVERT = "{:s}://{:s}{:s}{:s}"
    
    def __init__ (self, host, user, passwd, db_name="webrequest"):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db_name = db_name
                
        self._con = None
                
        self._initialize()
        
    def _create_database (self):
        self._con = _DBCon(self._host, self._user, self._passwd, None)
        
        with self._con as cur:
            sql = "CREATE DATABASE IF NOT EXISTS {:s};".format(
                    self._db_name
                )
            cur.execute(sql)
            
        self._con = _DBCon(self._host, self._user, self._passwd, self._db_name)
        
    def _create_domain_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain (
            domainid INTEGER UNSIGNED AUTO_INCREMENT,
            scheme CHAR(16),
            netloc CHAR(128),
            PRIMARY KEY (domainid),
            CONSTRAINT scheme_netloc_pair UNIQUE (scheme, netloc)
        );"""
        cur.execute(sql)
        
    def _create_url_table (self, cur):        
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
        cur.execute(sql)
        
    def _create_request_header_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS request_header (
            headerid INTEGER UNSIGNED AUTO_INCREMENT,
            headerchecksum BINARY(16) NOT NULL,
            header TEXT NOT NULL,
            
            PRIMARY KEY (headerid),
            CONSTRAINT unique_headerchecksum UNIQUE (headerchecksum)
        );"""
        cur.execute(sql)
        
    def _create_request_table (self, cur):
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
                REFERENCES request_header(headerid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
            CONSTRAINT unique_request UNIQUE (urlid, headerid, date)
        );"""
        cur.execute(sql)
        
    def _create_response_table (self, cur):
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
        cur.execute(sql)
        
    def _create_domain_timeout_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain_timeout ( 
            domainid INTEGER UNSIGNED NOT NULL,
            timeout_set DATETIME NOT NULL,
            timeout TIME NOT NULL,
            
            PRIMARY KEY (domainid, timeout_set),
            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
            CONSTRAINT unique_domain_timeout UNIQUE (domainid, timeout_set)
        );"""
        cur.execute(sql)
        
    def _create_full_request_view (self, cur):
        sql = """CREATE VIEW IF NOT EXISTS full_request AS 
        {:s};""".format(Storage.FULLREQUEST_QUERY)
        cur.execute(sql)
        
    def _create_domain_status_view (self, cur):
        sql = """CREATE VIEW IF NOT EXISTS domain_status AS
        {:s};""".format(Storage.DOMAINSTATUS_QUERY)
        cur.execute(sql)
        
    def _initialize (self):
        self._create_database()
        
        with self._con as cur:
            self._create_domain_table(cur)
            self._create_url_table(cur)
            
            self._create_request_header_table(cur)
            self._create_request_table(cur)
            self._create_response_table(cur)
            self._create_domain_timeout_table(cur)
            
            self._create_full_request_view(cur)
            self._create_domain_status_view(cur)
        
    def get_last_insert_id (self, cur=None):
        if cur is None:
            with self._con as cur:
                sql = "SELECT LAST_INSERT_ID();"
                cur.execute(sql)
                
                row = cur.fetchall()[0][0]
        else:
            sql = "SELECT LAST_INSERT_ID();"
            cur.execute(sql)
            
            row = cur.fetchall()[0][0]
            
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
            
            with self._con as cur:
                cur.execute(sql)
                last_id = self.get_last_insert_id(cur)
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
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            
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
            
            with self._con as cur:
                cur.execute(sql)
                
                last_id = self.get_last_insert_id(cur)
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
                
                with self._con as cur:
                    cur.execute(sql)
                    
                    rows = cur.fetchall()
                
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
        
        sql = """INSERT INTO request_header (headerchecksum, header)
        VALUES {:s};"""
        
        fmt = "(X\'{:s}\',\"{:s}\")"
        
        if isinstance(request_header, RequestHeader):
            fmt = fmt.format(
                request_header.header_checksum.hex(), 
                request_header.stringed_header_dict.replace("\"", "\\\""))
            sql = sql.format(fmt)
            
            with self._con as cur:
                cur.execute(sql)
                last_id = self.get_last_insert_id(cur)
        else:
            last_id = []
            
            for x in request_header:
                last_id.append(self.direct_insert_request_header(x))
                
            last_id = np.array(last_id)
            
        return last_id
    
    def get_request_header_id (self, request_header):
        if isinstance(request_header, RequestHeader):
            sql = """SELECT headerid
            FROM request_header
            WHERE headerchecksum = X'{:s}';          
            """.format(request_header.header_checksum.hex())
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            
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
            
            with self._con as cur:
                cur.execute(sql)                
                last_id = self.get_last_insert_id(cur)
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
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            
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
        if isinstance(response, Response):
            content = response.content
            
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
            
            with self._con as cur:
                cur.execute(sql)
                last_id = self.get_last_insert_id(cur)
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
    
    def get_latest_response (self, request_id, status_code=None):
        if status_code is None:
            sc_sql = ""
        else:
            sc_sql = " AND statuscode = {:d}".format(status_code)
        
        sql = """SELECT responseid, requestid, requested, statuscode, header, content
        FROM response
        WHERE requestid = {:d}{:s}
        ORDER BY requested DESC LIMIT 1;
        """.format(request_id, sc_sql)
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        if len(rows) == 1:
            df = pd.Series(rows[0], index=["ResponseId", "RequestId", "Timestamp", "StatusCode", "Header", "Content"])
        else:
            df = None
        
        return df
    
    @classmethod
    def _prepare_fullrequest_dataframe (cls, rows, additional_columns=[]):
        all_columns = Storage.FULLREQUEST_COLUMNS + additional_columns
        
        df = pd.DataFrame(rows, columns=all_columns)
        df = df.set_index(Storage.FULLREQUEST_INDEX)
        
        return df
        
    def get_requests_without_responses (self):
        sql = """SELECT
            fr.requestid, fr.urlid, fr.domainid, fr.headerid,
            fr.scheme, fr.netloc, fr.path, fr.query,
            fr.header,
            fr.date, fr.url
        FROM ({:s}) fr
        LEFT JOIN response AS resp
            ON fr.requestid = resp.requestid
        GROUP BY fr.requestid
        HAVING COUNT(resp.responseid) = 0;
        """.format(Storage.FULLREQUEST_QUERY)
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df
    
    @classmethod
    def query_requests_without_coded_responses (cls, status_code=200):
        sql = """SELECT
            fr.requestid, fr.urlid, fr.domainid, fr.headerid,
            fr.scheme, fr.netloc, fr.path, fr.query,
            fr.header,
            fr.date, fr.url
        FROM ({:s}) fr
        LEFT JOIN response AS resp
            ON (fr.requestid = resp.requestid AND resp.statuscode = {:d})
        GROUP BY fr.requestid
        HAVING COUNT(resp.responseid) = 0
        """.format(cls.FULLREQUEST_QUERY, status_code)
        return sql
    
    def get_requests_without_coded_responses (self, status_code=200):
        sql = "{:s};".format(
                Storage.query_requests_without_coded_responses(status_code)
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df
    
    def get_domain_status (self):
        sql = "{:s};".format(Storage.DOMAINSTATUS_QUERY)
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = pd.DataFrame(rows, columns=Storage.DOMAINSTATUS_COLUMNS)
        df = df.set_index(Storage.DOMAINSTATUS_INDEX)
        return df        
    
    @classmethod
    def timedelta_to_string (cls, td):
        total_secs = int(td.total_seconds())
        
        positive = total_secs >= 0
        positive = "" if positive == True else "-"
        
        total_secs = np.abs(total_secs).astype(int)
        
        hours, total_secs = divmod(total_secs, 3600)
        minutes, seconds = divmod(total_secs, 60)
        
        td = "{:s}{:02d}:{:02d}:{:02d}".format(
                positive, hours, 
                minutes, seconds)
        
        return td
    
    def direct_insert_domain_timeout (self, domain_id, timeout):
        fmt = "({:d}, NOW(), \"{:s}\")"
        sql = """INSERT INTO domain_timeout (domainid, timeout_set, timeout)
            VALUES {:s};"""
        
        if isinstance(timeout, dt.timedelta):
            timeout = Storage.timedelta_to_string(timeout)
            
            sql = sql.format(fmt.format(domain_id, timeout))
        else:
            if len(timeout) != 0:
                fmt = ",".join(
                        fmt.format(di, Storage.timedelta_to_string(to))
                        for di, to in zip(domain_id, timeout)
                    )
                sql = sql.format(fmt)
            else:
                return
            
        print(sql)
        
        with self._con as cur:
            cur.execute(sql)
        
    def get_latest_domain_timeout (self):
        sql = """{:s};""".format(Storage.LATESTTIMEOUT_QUERY)
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = pd.DataFrame(rows, columns=Storage.DOMAINTIMEOUT_COLUMNS)
        df = df.set_index(Storage.DOMAINTIMEOUT_INDEX)
        
        return df
    
    def get_domain_ids_without_domain_timeouts (self):
        sql = """SELECT x.domainid
        FROM 
        (SELECT d.domainid, SUM(dt.domainid IS NOT NULL) "count"
        FROM domain AS d
        LEFT JOIN domain_timeout AS dt
            ON d.domainid = dt.domainid
        GROUP BY d.domainid) x
        WHERE x.count = 0;"""
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
        rows = np.array([
                x[0]
                for x in rows
            ])
        return rows
    
    def get_failing_request_timeouts (self, status_code=200):
        sql = """SELECT 
            fresp.requestid, fresp.urlid, 
            fresp.domainid, fresp.headerid,
            fresp.scheme, fresp.netloc, 
            fresp.path, fresp.query, 
            fresp.header, fresp.date,
            fresp.url,
            ADDTIME(domstatus.requested, TIME_TO_SEC(ltimeouts.timeout)) "retry"
        FROM ({:s}) fresp
        INNER JOIN ({:s}) domstatus
        ON fresp.domainid = domstatus.domainid AND fresp.headerid = domstatus.headerid
        INNER JOIN ({:s}) ltimeouts
        ON fresp.domainid = ltimeouts.domainid
        """.format(
                Storage.query_requests_without_coded_responses(status_code),
                Storage.DOMAINSTATUS_QUERY,
                Storage.LATESTTIMEOUT_QUERY
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows, [Storage.RETRY_COLUMN])
        
        return df
    
    def get_retryable_failing_request (self, status_code=200):
        sql = """SELECT 
            fresp.requestid, fresp.urlid, 
            fresp.domainid, fresp.headerid,
            fresp.scheme, fresp.netloc, 
            fresp.path, fresp.query, 
            fresp.header, fresp.date,
            fresp.url
        FROM ({:s}) fresp
        INNER JOIN ({:s}) domstatus
        ON fresp.domainid = domstatus.domainid AND fresp.headerid = domstatus.headerid
        INNER JOIN ({:s}) ltimeouts
        ON fresp.domainid = ltimeouts.domainid
        WHERE ADDTIME(domstatus.requested, TIME_TO_SEC(ltimeouts.timeout)) <= NOW()
        """.format(
                Storage.query_requests_without_coded_responses(status_code),
                Storage.DOMAINSTATUS_QUERY,
                Storage.LATESTTIMEOUT_QUERY
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df