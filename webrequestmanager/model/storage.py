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
import datetime as dt
import time
from threading import RLock

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
    def __init__ (self, url, request_header, timestamp, accepted_status):
        self.url = url
        self.request_header = request_header
        self.timestamp = timestamp
        self.accepted_status = accepted_status
        
class Response ():
    def __init__ (self, request, status_code, timestamp, headers, content):
        self.request = request
        self.status_code = status_code
        self.timestamp = timestamp
        self.headers = headers
        self.content = content
        
    @classmethod
    def of_response (cls, request, requests_response, timestamp):
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
            
        return Response(request, status_code, timestamp, headers, content)
    
    def is_accepted (self, accepted_status_codes):
        return self.status_code in accepted_status_codes
    
class _DBCon ():
    def __init__ (self, host, user, passwd, db_name):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db_name = db_name
        
        self._lock = RLock()
        self._con = None
        
    def __enter__ (self):
        self._lock.acquire(blocking=True)
        
        attempts = 1000
        last_attempt = attempts - 1
        
        for attempt in range(attempts):
            try:
                self._con = mysql.connector.connect(
                        host=self._host,
                        user=self._user,
                        password=self._passwd,
                        database=self._db_name
                    )
                return self._con.cursor()
            except Exception as e:
                if attempt != last_attempt:
                    if attempt % 100 == 0:
                        print("Failed to connect - {:d}: {:s}".format(
                                attempt, str(e)
                            ))
                        
                    time.sleep(1)
                else:
                    raise e
        
    
    def __exit__ (self, exc_type, exc_val, exc_tb):
        self._con.commit()
        self._con.close()
        self._lock.release()
        
        
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
        d_s.domainid, d_s.headerid,
        d.scheme, d.netloc, reqh.header,
        d_s.requested, d_s.status
    FROM domain_status AS d_s
    INNER JOIN domain AS d
        ON d_s.domainid = d.domainid
    INNER JOIN request_header AS reqh
        ON d_s.headerid = reqh.headerid"""
    DOMAINSTATUS_COLUMNS = ["DomainId", "HeaderId", "Scheme", "Netloc", 
                            "Header", "Timestamp", "Status"]
    DOMAINSTATUS_INDEX = ["DomainId", "HeaderId"]
    
    DOMAINTIMEOUT_COLUMNS = ["DomainId", "Timeout"]
    DOMAINTIMEOUT_INDEX = "DomainId"
    
    RETRY_COLUMN = "Retry"
    
    RESPONSE_COLUMNS = ["ResponseId", "RequestId", "Timestamp", "StatusCode", "Header", "Content"]
    
    FULLREQUEST_PENDING_QUERY = """SELECT req.requestid, req.urlid, 
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
                    ) "url",
                    x.completing_responseid,
                    y.response_count
    FROM request AS req
    INNER JOIN url 
                ON req.urlid = url.urlid 
            INNER JOIN domain AS d 
                ON url.domainid = d.domainid 
            INNER JOIN request_header AS reqh 
                ON req.headerid = reqh.headerid
    LEFT JOIN (SELECT req.requestid, resp.responseid "completing_responseid"
        FROM request AS req
        INNER JOIN response AS resp
            ON req.requestid = resp.requestid
        INNER JOIN accepted_status AS a_s
            ON req.requestid = a_s.requestid AND resp.statuscode = a_s.status
    ON req.requestid = x.requestid
    LEFT JOIN (SELECT req.requestid, COUNT(resp.responseid) "response_count"
                FROM request AS req
                LEFT JOIN response AS resp
                ON req.requestid = resp.requestid
              GROUP BY req.requestid) y
    ON req.requestid = y.requestid
    WHERE x.completing_responseid IS NULL"""
    FULLREQUEST_PENDING_COLUMNS = ["RequestId", "UrlId", "DomainId", "HeaderId", "Scheme", "Netloc",
                           "Path", "Query", "Header", "Timestamp", "URL", "CompletingResponseId",
                           "ResponseCount"]
    FULLREQUEST_PENDING_INDEX = ["RequestId", "UrlId", "DomainId", "HeaderId"]
    
    LAST_REQUEST_RESPONSE_QUERY = """SELECT req.requestid, resp.responseid, x.requested, resp.statuscode
        FROM request AS req
        INNER JOIN (SELECT resp.requestid, MAX(resp.requested) "requested"
                   FROM response AS resp
                   GROUP BY resp.requestid) x
        ON req.requestid = x.requestid
        INNER JOIN response AS resp
            ON x.requestid = resp.requestid AND x.requested = resp.requested"""
    
    COMPLETING_RESPONSES_QUERY = """SELECT resp.responseid, req.requestid, resp.requested,
        resp.statuscode IN (SELECT a_s.statuscode FROM accepted_status AS a_s WHERE a_s.requestid = req.requestid) "accepted"
    FROM request AS req
    INNER JOIN response AS resp
        ON req.requestid = resp.requestid"""
        
    LAST_COMPLETING_RESPONSES_QUERY = """SELECT x.requestid, x.requested, y.responseid, y.accepted
    FROM (SELECT req.requestid, MAX(resp.requested) "requested"
          FROM request AS req
          INNER JOIN response AS resp
              ON req.requestid = resp.requestid
          GROUP BY req.requestid) x
          
    INNER JOIN (SELECT resp.responseid, req.requestid, resp.requested,
        resp.statuscode IN (SELECT a_s.statuscode FROM accepted_status AS a_s WHERE a_s.requestid = req.requestid) "accepted"
        FROM request AS req
        INNER JOIN response AS resp
            ON req.requestid = resp.requestid) y
    ON x.requestid = y.requestid AND x.requested = y.requested
    """
    
    # Query for getting all responses for completed requests
    # which did not match any accepted http status codes.
    FAILED_RESPONSES_QUERY = """SELECT resp.requestid, resp.responseid
    FROM response AS resp
    INNER JOIN request_status AS reqs
        ON resp.requestid = reqs.requestid
    WHERE reqs.status = 2 AND resp.statuscode NOT IN (
        SELECT acs.statuscode
        FROM accepted_status AS acs
        WHERE acs.requestid = resp.requestid
    )"""
    
    DOMAIN_POLICY_QUERY = """
    SELECT domainid, timeout, retries, retry_mindelay, retry_maxdelay,
        retry_http, retry_proxies, bpslimit, proxy_default, proxy_regions
    FROM domain_policy
    """
    DOMAIN_POLICY_COLUMNS = ["DomainId", "Timeout", "Retries", "RetryMinDelay",
            "RetryMaxDelay", "RetryHTTP", "RetryProxies", "BPSLimit", "ProxyDefault",
            "ProxyRegions"]
    DOMAIN_POLICY_INDEX = "DomainId"

    REQUESTSTATUS_QUERY = """SELECT requestid, requested, status
    FROM request_status"""
    REQUESTSTATUS_COLUMNS = ["RequestId", "Requested", "Status"]
    REQUESTSTATUS_INDEX = "RequestId"

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
    
    def _create_domain_policy_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain_policy (
            domainid INTEGER UNSIGNED PRIMARY KEY,
            timeout SMALLINT UNSIGNED DEFAULT 30,

            retries TINYINT UNSIGNED DEFAULT 2,
            retry_mindelay SMALLINT UNSIGNED DEFAULT 0,
            retry_maxdelay SMALLINT UNSIGNED DEFAULT 0,
            retry_http TINYINT UNSIGNED DEFAULT 0,
            retry_proxies TINYINT UNSIGNED DEFAULT 0,

            bpslimit INT UNSIGNED DEFAULT NULL,

            proxy_default TINYINT UNSIGNED DEFAULT 0,
            proxy_regions TEXT NULL,

            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
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
                    ON UPDATE NO ACTION,
            INDEX(statuscode),
            INDEX(requestid, requested)
        );"""
        cur.execute(sql)
    
    def _create_accepted_status_codes_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS accepted_status (
            requestid INTEGER UNSIGNED NOT NULL,
            statuscode SMALLINT UNSIGNED NOT NULL,
        
            PRIMARY KEY (requestid, statuscode),
            FOREIGN KEY (requestid)
                REFERENCES request(requestid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION,
            CONSTRAINT unique_accepted_status_codes UNIQUE (requestid, statuscode)
        );"""
        cur.execute(sql)
    
    def _create_domain_timeout_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain_timeout ( 
            domainid INTEGER UNSIGNED NOT NULL,
            timeout TIME NOT NULL,
            
            PRIMARY KEY (domainid),
            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
        );"""
        cur.execute(sql)
        
    def _create_request_status_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS request_status ( 
            requestid INTEGER UNSIGNED NOT NULL,
            requested DATETIME NULL,
            status TINYINT UNSIGNED NOT NULL,
            
            PRIMARY KEY (requestid),
            FOREIGN KEY (requestid)
                REFERENCES request(requestid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
            INDEX(status)
        );"""
        cur.execute(sql)
        
    def _create_domain_status_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain_status (
            domainid INTEGER UNSIGNED,
            headerid INTEGER UNSIGNED,
            requested DATETIME NULL,
            status TINYINT UNSIGNED NOT NULL,
            
            PRIMARY KEY (domainid, headerid),
            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
            FOREIGN KEY (headerid)
                REFERENCES request_header(headerid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
        );"""
        cur.execute(sql)
        
    def _create_domain_retry_table (self, cur):
        sql = """CREATE TABLE IF NOT EXISTS domain_retry (
            domainid INTEGER UNSIGNED,
            headerid INTEGER UNSIGNED,
            retry DATETIME NULL,
            
            PRIMARY KEY (domainid, headerid),
            FOREIGN KEY (domainid)
                REFERENCES domain(domainid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
            FOREIGN KEY (headerid)
                REFERENCES request_header(headerid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
            INDEX(retry)
        );"""
        cur.execute(sql)
        
    def _create_full_request_view (self, cur):
        sql = "DROP VIEW IF EXISTS full_request;"
        cur.execute(sql)

        sql = """CREATE VIEW full_request AS 
        {:s};""".format(Storage.FULLREQUEST_QUERY)
        cur.execute(sql) 
    
    def _create_domain_insert_trigger (self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_domain_trigger;"
        cur.execute(sql)

        sql = """CREATE TRIGGER insert_domain_trigger
        AFTER INSERT
        ON domain
        FOR EACH ROW
        INSERT INTO domain_policy (domainid) VALUES (NEW.domainid);
        """
        cur.execute(sql)

    def _create_request_insert_trigger (self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_request_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER insert_request_trigger
        AFTER INSERT
        ON request
        FOR EACH ROW
        INSERT INTO request_status (requestid, requested, status) 
        VALUES (NEW.requestid, NULL, 0);
        """
        cur.execute(sql)
        
    def _create_response_insert_trigger (self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_response_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER insert_response_trigger
        AFTER INSERT
        ON response
        FOR EACH ROW
        UPDATE IGNORE request_status
        SET status = 1 + (SELECT NEW.statuscode IN 
            (SELECT a_s.statuscode FROM accepted_status AS a_s WHERE a_s.requestid = NEW.requestid)),
            requested = NEW.requested
        WHERE requestid = NEW.requestid
        """
        cur.execute(sql)
        
    def _create_request_status_insert_trigger (self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_request_status_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER insert_request_status_trigger
        AFTER INSERT
        ON request_status
        FOR EACH ROW
        INSERT IGNORE INTO domain_status (domainid, headerid, requested, status) 
        SELECT d.domainid, req.headerid, NULL, 0
        FROM request AS req
        INNER JOIN url
            ON req.urlid = url.urlid
        INNER JOIN domain AS d
            ON url.domainid = d.domainid
        WHERE req.requestid = NEW.requestid
        """
        cur.execute(sql)
        
    def _create_request_status_update_trigger (self, cur):
        sql = "DROP TRIGGER IF EXISTS update_request_status_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER update_request_status_trigger
        AFTER UPDATE
        ON request_status
        FOR EACH ROW
        UPDATE IGNORE domain_status
        SET requested = NEW.requested,
            status = NEW.status
        WHERE (domainid, headerid) IN (
                SELECT d.domainid, req.headerid
                FROM request AS req
                INNER JOIN url
                    ON req.urlid = url.urlid
                INNER JOIN domain AS d
                    ON url.domainid = d.domainid
                WHERE req.requestid = NEW.requestid
            )
        """
        cur.execute(sql)
        
    def _create_domain_status_update_trigger (self, cur):
        # domainid, headerid, requested, status
        # TIMESTAMPADD(SECOND, TIME_TO_SEC(dt.timeout), d_s.requested) "retry"
        sql = "DROP TRIGGER IF EXISTS insert_domain_status_trigger;"
        cur.execute(sql)


        sql = """
        CREATE TRIGGER insert_domain_status_trigger
        AFTER UPDATE
        ON domain_status
        FOR EACH ROW
        INSERT INTO domain_retry (domainid, headerid, retry)
        SELECT 
            NEW.domainid, NEW.headerid, 
            TIMESTAMPADD(SECOND, TIME_TO_SEC(dt.timeout), NEW.requested) "retry"
        FROM domain_timeout AS dt
        WHERE dt.domainid = NEW.domainid AND NEW.status = 1
        ON DUPLICATE KEY UPDATE
            retry = (SELECT 
                TIMESTAMPADD(SECOND, TIME_TO_SEC(dt.timeout), NEW.requested) "retry"
            FROM domain_timeout AS dt
            WHERE dt.domainid = NEW.domainid AND NEW.status = 1)
        """
        cur.execute(sql)
        
    def _initialize (self):
        self._create_database()
        
        with self._con as cur:
            self._create_domain_table(cur)
            self._create_domain_policy_table(cur)
            self._create_url_table(cur)
            
            self._create_request_header_table(cur)
            self._create_request_table(cur)
            self._create_request_status_table(cur)
            self._create_response_table(cur)
            self._create_accepted_status_codes_table(cur)
            self._create_domain_timeout_table(cur)
            self._create_domain_status_table(cur)
            self._create_domain_retry_table(cur)
            
            self._create_full_request_view(cur)
            
            self._create_domain_insert_trigger(cur)
            self._create_request_insert_trigger(cur)
            self._create_response_insert_trigger(cur)
            self._create_request_status_insert_trigger(cur)
            self._create_request_status_update_trigger(cur)
            self._create_domain_status_update_trigger(cur)
        
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
            
    def direct_insert_domain_policy (self, domain_id, timeout=None, retries=None,
                                    retry_mindelay=None, retry_maxdelay=None,
                                    retry_http=None, retry_proxies=None,
                                    bps_limit=None, proxy_default=None,
                                    proxy_regions=None):
        sql = """INSERT INTO domain_policy {:s} 
                VALUES {:s}  {:s};"""

        column_list = ["domainid"]
        value_list = [str(domain_id)]        
        bool_to_str = lambda x: "1" if x == True else "0"

        if timeout is not None:
            column_list.append("timeout")
            value_list.append(str(timeout))    

        if retries is not None:
            column_list.append("retries")
            value_list.append(str(retries))

        if retry_mindelay is not None:
            column_list.append("retry_mindelay")
            value_list.append(str(retry_mindelay))

        if retry_maxdelay is not None:
            column_list.append("retry_maxdelay")
            value_list.append(str(retry_maxdelay))

        if retry_http is not None:
            column_list.append("retry_http")
            value_list.append(bool_to_str(retry_http))

        if retry_proxies is not None:
            column_list.append("retry_proxies")
            value_list.append(bool_to_str(retry_proxies))

        if bps_limit is not None:
            bps_limit = "NULL" if bps_limit < 0 else f"\"{bps_limit}\""
            column_list.append("bpslimit")
            value_list.append(bps_limit)

        if proxy_default is not None:
            column_list.append("proxy_default")
            value_list.append(bool_to_str(proxy_default))

        if proxy_regions is not None:
            column_list.append("proxy_regions")
            value_list.append(f"\"{proxy_regions}\"")

        value_list = "({:s})".format(",".join(value_list))

        if len(column_list) > 1:
            update = ",".join("{:s} = VALUES({:s})".format(x, x) for x in column_list[1:])
            update = "ON DUPLICATE KEY UPDATE "+update
        else:
            update = ""
    
        column_list = "({:s})".format(",".join(column_list))

        sql = sql.format(column_list, value_list, update)

        with self._con as cur:
            cur.execute(sql)
    
    def get_domain_policy (self):
        sql = "{:s};".format(Storage.DOMAIN_POLICY_QUERY)

        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=Storage.DOMAIN_POLICY_COLUMNS)
        df = df.set_index(Storage.DOMAIN_POLICY_INDEX)
        return df




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
            
    def direct_insert_accepted_status (self, request_id, status_code):
        sql = "INSERT IGNORE INTO accepted_status (requestid, statuscode) VALUES {:s};"
        
        fmt = "({:d},{:d})"
        
        rid_list = isinstance(request_id, (list, np.ndarray))
        sc_list = isinstance(status_code, (list, np.ndarray))
        
        if not rid_list and sc_list:
            values = ",".join(fmt.format(request_id, x) for x in status_code)
        elif rid_list and not sc_list:
            values = ",".join(fmt.format(x, status_code) for x in request_id)
        else:
            values = fmt.format(request_id, status_code)
            
        sql = sql.format(values)
        
        with self._con as cur:
            cur.execute(sql)
            
    def get_accepted_status (self, request_id):
        sql = "SELECT statuscode FROM accepted_status WHERE requestid = {:d}".format(
                request_id
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
        rows = [x[0] for x in rows]
        return rows
            
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
            
            for ui, hi, ts in zip(url_id, header_id, timestamp):
                last_id.append(self.direct_insert_request(ui, hi, ts))
                
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
                
            self.direct_insert_accepted_status(existing_id, request.accepted_status)
                
            return existing_id
        
    def direct_insert_response (self, request_id, response):
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
                    response.timestamp.strftime(Storage.DATETIME_FORMAT),
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
            
            for ri, r in zip(request_id, response):
                last_id.append(self.direct_insert_response(ri, r))
                
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
    
    def get_latest_accepted_response (self, request_id):
        sql = """SELECT 
            resp.responseid, resp.requestid, resp.requested, 
            resp.statuscode, resp.header, resp.content
        FROM response AS resp
        WHERE resp.requestid = {:d} AND 
        resp.statuscode IN (
            SELECT statuscode 
            FROM accepted_status 
            WHERE resp.requestid = {:d}
        )
        ORDER BY resp.requested DESC LIMIT 1;""".format(
            request_id, request_id
            )
        ["ResponseId", "RequestId", "Timestamp", "StatusCode", "Header", "Content"]
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        if len(rows) == 1:
            df = pd.Series(rows[0], index=Storage.RESPONSE_COLUMNS)
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
        FROM request_status AS rs
        INNER JOIN ({:s}) fr
            ON rs.requestid = fr.requestid
        LEFT JOIN domain_retry AS dr
            ON fr.domainid = dr.domainid AND fr.headerid = dr.headerid
        WHERE rs.status = 0 
            AND (dr.retry <= UTC_TIMESTAMP() OR dr.retry IS NULL);
        """.format(Storage.FULLREQUEST_QUERY)
        
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
        fmt = "({:d}, \"{:s}\")"
        sql = """INSERT INTO domain_timeout (domainid, timeout)
            VALUES {:s} ON DUPLICATE KEY UPDATE timeout = VALUES(timeout);"""
        
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
        
        with self._con as cur:
            cur.execute(sql)
        
    def get_domain_timeout (self):
        sql = "SELECT domainid, timeout FROM domain_timeout;"
        
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
    
    def get_failing_request_timeouts (self):
        sql = """SELECT 
            fr.requestid, fr.urlid, 
            fr.domainid, fr.headerid,
            fr.scheme, fr.netloc, 
            fr.path, fr.query, 
            fr.header, fr.date,
            fr.url,
            TIMESTAMPADD(SECOND, TIME_TO_SEC(dt.timeout), d_s.requested) "retry"
        FROM ({:s}) fr
        INNER JOIN request_status AS reqs
            ON fr.requestid = reqs.requestid
        INNER JOIN domain_timeout AS dt
            ON fr.domainid = dt.domainid
        INNER JOIN domain_status AS d_s
            ON fr.domainid = d_s.domainid AND fr.headerid = d_s.headerid
        WHERE reqs.status = 1
        """.format(
                Storage.FULLREQUEST_QUERY
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows, [Storage.RETRY_COLUMN])
        
        return df
    
    def get_retryable_failing_request (self):
        sql = """SELECT 
            fr.requestid, fr.urlid, 
            fr.domainid, fr.headerid,
            fr.scheme, fr.netloc, 
            fr.path, fr.query, 
            fr.header, fr.date,
            fr.url
        FROM ({:s}) fr
        INNER JOIN request_status AS rs
            ON fr.requestid = rs.requestid AND rs.status = 1
        LEFT JOIN domain_retry AS dr
            ON fr.domainid = dr.domainid AND fr.headerid = dr.headerid
        WHERE dr.retry <= UTC_TIMESTAMP() OR dr.retry IS NULL
        """.format(
                Storage.FULLREQUEST_QUERY
            )
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        
        df = Storage._prepare_fullrequest_dataframe(rows)
        
        return df

    def get_request_status(self):
        sql = "{:s};".format(Storage.REQUESTSTATUS_QUERY)
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=Storage.REQUESTSTATUS_COLUMNS)
        df = df.set_index(Storage.REQUESTSTATUS_INDEX)
        return df

    def fill_missing_request_statuses(self):
        sql = """INSERT INTO request_status (requestid, requested, status)
        SELECT r.requestid, r.date, 0
        FROM request AS r
        LEFT JOIN request_status AS rs
            ON r.requestid = rs.requestid
        WHERE rs.requestid IS NULL;"""

        with self._con as cur:
            cur.execute(sql)


