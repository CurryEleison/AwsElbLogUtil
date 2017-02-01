import boto3
import os
from io import BytesIO

from datetime import tzinfo, datetime, timedelta
import csv
import pandas as pd
import numpy as np
from urlparse import urlparse
import hashlib
import urllib2
import json
import gzip

ZERO = timedelta(0)


#A class to fetch a list of logfilesd from S3 object summaries and put them into a dataframe
class LogDataFrame:
    """LogDataFrame"""

    def __init__(self, s3res, loglineclass = None):
        self.s3res = s3res
        self.ctor = loglineclass if loglineclass != None else LogLine
        self.delimiter = self.ctor.get_delimiter()
        self.commenter = self.ctor.get_isdataline()
        self.gzipped = self.ctor.get_gzipped()

    # Maybe add an argument to exclude lines. Perhaps a lambda that takes 
    # a LogLine object and returns a bool or something to scan a string and return a bool
    def make_dataframe(self, s3items, loglinefilter=None):
        loglines = []
        s3client = boto3.client('s3')
        for s3objsummary in s3items:
            # Had real trouble using download_life
            s3obj = s3client.get_object(Bucket = s3objsummary.bucket_name, Key = s3objsummary.key)
            bytestream = BytesIO(s3obj['Body'].read())
            bf = gzip.GzipFile(None, 'rb', fileobj=bytestream) if self.gzipped else bytestream
            bf.seek(0)
            self.append_lines(bf, loglines, loglinefilter)
            bf.close()
            if not bytestream.closed:
                bytestream.close()
        return self.get_df_from_loglines(loglines)

    def make_dataframe_fromfolder(self, foldername, loglinefilter=None):
        opener = gzip.open if self.gzipped else open
        fullfoldername = os.path.expanduser(foldername)
        if os.path.exists(fullfoldername):
            loglines = [] 
            filelist = [f for f in os.listdir(fullfoldername) if os.path.isfile(os.path.join(fullfoldername, f))]
            for filename in filelist: 
                with opener(os.path.join(fullfoldername, filename), 'rb') as buf:
                    self.append_lines(buf, loglines, loglinefilter)
            return self.get_df_from_loglines(loglines)
        else:
            print "Could not find folder {0}".format(fullfoldername)



    def append_lines(self, f, loglines, loglinefilter):
        ctor = self.ctor
        cmts = self.commenter
        delim = self.delimiter
        csvreader = csv.reader(filter(cmts, f), delimiter = delim, quotechar = '"')
        for row in csvreader:
            l = ctor(row)
            if ((loglinefilter == None) or (loglinefilter(l) == True)):
                loglines.append(l)

    def get_df_from_loglines(self, loglines):
        if len(loglines) > 0:
            variables = loglines[0].__dict__.keys()
            return pd.DataFrame([[getattr(i,j) for j in variables] for i in loglines], columns = variables)



# A class to download a list of S3 log files to a folder
class LogFileDownloader:
    """LogFileDownLoader"""

    def __init__(self, s3res, folder, skipexisting = True):
        self.folder = folder
        self.s3res = s3res
        self.skipexisting = skipexisting

    def download_logs(self, s3items):
        fullfoldername = os.path.expanduser(self.folder)
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
        for s3objsummary in s3items:
            s3obj = self.s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
            s3objfilename = os.path.basename(s3objsummary.key)
            logfiletarget = os.path.expanduser(os.path.join(fullfoldername, s3objfilename))
            if os.path.exists(logfiletarget): 
                # Tried checking ETags, but those are not md5 sums for ELB log files
                if self.skipexisting:
                    continue
                else:
                    os.remove(logfiletarget)
            s3obj.download_file(logfiletarget)



#A class to get a few recent ELB logfiles from S3
class LogFileList:
    """LogFileList"""

    def __init__(self, s3res, account = None, region = 'eu-west-1', 
            bucket = "123logging", minimumfiles = 5, strictreftime = False,
            lbtype = 'elb'):
        self.account = account if account != None else self.get_awsacctno()
        self.region = region
        self.minimumfiles = minimumfiles
        self.s3res = s3res
        self.bucket = bucket
        self.strictreftime = strictreftime
        self.lbtype = lbtype

    def get_recents(self, distribution, refdate = None, logfolder = None):
        if (self.lbtype == 'alb'):
            return self.get_recents_alb(distribution, refdate = refdate, lblogfolder = logfolder)
        elif (self.lbtype == 'cf'):
            return self.get_recents_cloudfront(distribution, refdate = refdate, cflogfolder = logfolder)
        else:
            return self.get_recents_elb(distribution, refdate = refdate, lblogfolder = logfolder)


    def get_recents_alb(self, lbname, refdate = None, lblogfolder = None):
        logfolder = lblogfolder if lblogfolder != None else lbname
        s3foldertemplate = "loadbalancers/{loadbalancer}/AWSLogs/{account}/elasticloadbalancing/{region}/{dt.year:0>4}/{dt.month:0>2}/{dt.day:0>2}/"
        s3filekeyroottemplate = "{account}_elasticloadbalancing_{region}_app.{loadbalancer}"
        def folderpref(dt): return s3foldertemplate.format(dt = dt, loadbalancer = logfolder, account = self.account, region = self.region) 
        def filepref(dt): return s3filekeyroottemplate.format(dt = dt, loadbalancer = lbname, account = self.account, region = self.region ) 
        def prefix(dt): return folderpref(dt) + filepref(dt) 
        return self._get_recents(prefix, refdate)


    def get_recents_cloudfront(self, distribution, refdate=None, cflogfolder = None):
        folderprefix = (cflogfolder + "/") if cflogfolder != None else ""
        def prefix(dt): return "{foldpref}{filepref}.{dt.year:0>4}-{dt.month:0>2}-{dt.day:0>2}-{dt.hour:0>2}".format(dt = dt, foldpref = folderprefix, filepref = distribution)
        return self._get_recents(prefix, refdate)


    def get_recents_elb(self, lbname, refdate=None, lblogfolder = None):
        logfolder = lblogfolder if lblogfolder != None else lbname
        s3foldertemplate = "loadbalancers/{loadbalancer}/AWSLogs/{account}/elasticloadbalancing/{region}/{dt.year:0>4}/{dt.month:0>2}/{dt.day:0>2}/"
        s3filekeyroottemplate = "{account}_elasticloadbalancing_{region}_{loadbalancer}_{dt.year:0>4}{dt.month:0>2}{dt.day:0>2}T{dt.hour:0>2}"
        def folderpref(dt): return s3foldertemplate.format(dt = dt, loadbalancer = logfolder, account = self.account, region = self.region)
        def filepref(dt): return s3filekeyroottemplate.format(dt = dt, loadbalancer = lbname, account = self.account, region = self.region )
        def prefix(dt): return folderpref(dt) + filepref(dt)
        return self._get_recents(prefix, refdate)


    def _get_recents(self, prefixer, refdate=None):
        utc = UTC()
        allitems = []
        checkedkeys = set()
        iterations = 0
        maxiterations = 500
        tenminspast = timedelta(minutes=-10)
        starttime = refdate if refdate != None else datetime.now(utc) 
        mytime = starttime
        while (len(allitems) <= self.minimumfiles and iterations < maxiterations):
            fullprefix = prefixer(mytime)
            if (fullprefix not in checkedkeys):
                print fullprefix
                bucket = self.s3res.Bucket(self.bucket)
                allitems.extend( filter( lambda item: (self.strictreftime == False) or (refdate == None) or (item.last_modified < refdate), sorted( bucket.objects.filter(Prefix=fullprefix), key = lambda item: item.last_modified, reverse=True)))
                checkedkeys.add(fullprefix)
            iterations += 1
            mytime += tenminspast
        recents = [x for ind, x in enumerate(allitems) if self.minimumfiles > ind >= 0 ]
        return recents

    def get_awsacctno(self):
        client = boto3.client("sts")
        account_id = client.get_caller_identity()["Account"]
        return account_id


# A UTC class. From tzinfo docs
class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

# A class to hold a log line
class LogLine:
    """LogLine"""

    @staticmethod
    def get_delimiter():
        return ' '

    @staticmethod
    def get_gzipped():
        return False

    @staticmethod
    def get_isdataline():
        return lambda row: True

    def __init__(self, fields):
        self.utctime = datetime.strptime(fields[0], '%Y-%m-%dT%H:%M:%S.%fZ')
        self.loadbalancer = fields[1]
        (self.remoteip, self.remoteport) = fields[2].split(':')
        if (fields[3] != '-'):
            (self.hostip, self.hostport) = fields[3].split(':')
        else:
            (self.hostip, self.hostport) = ("-", "-1")
        self.time1 = float(fields[4])
        self.servertime = float(fields[5])
        self.time2 = float(fields[6])
        self.timetaken = (self.time1 + self.servertime + self.time2) if self.servertime > 0 else self.servertime
        self.responsecode = int(fields[7])
        self.responsebytes = int(fields[10])
        reqfields = fields[11].split(' ')
        if len(reqfields) >= 2:
            self.method = reqfields[0]
            self.url = reqfields[1]
            if (len(reqfields) >=3):
                self.protocol = reqfields[2]
            else:
                self.protocol = "unknown"
        else:
            print reqfields
        u = urlparse(self.url)
        self.hostname = u.hostname
        self.path = u.path
        self.useragent = fields[12]
        self.encryption = fields[13]


class AlbLogLine:
    """AblLogLine"""

    @staticmethod
    def get_delimiter():
        return ' '

    @staticmethod
    def get_gzipped():
        return True

    @staticmethod
    def get_isdataline():
        return lambda row: True

    def __init__(self, fields):
        self.protocoltype = fields[0]
        self.utctime = datetime.strptime(fields[1], '%Y-%m-%dT%H:%M:%S.%fZ')
        self.loadbalancer = fields[2]
        (self.remoteip, self.remoteport) = fields[3].split(':')
        if (fields[3] != '-'):
            (self.hostip, self.hostport) = fields[3].split(':')
        else:
            (self.hostip, self.hostport) = ("-", "-1")
        self.time1 = float(fields[5]) 
        self.servertime = float(fields[6]) if float(fields[6]) > 0 else None
        self.time2 = float(fields[7])
        self.timetaken = (self.time1 + self.servertime + self.time2) if self.servertime > 0 else self.servertime
        self.responsecode = int(fields[8])
        self.responsebytes = int(fields[11])
        reqfields = fields[12].split(' ')
        if len(reqfields) >= 2:
            self.method = reqfields[0]
            self.url = reqfields[1]
            if (len(reqfields) >=3):
                self.protocol = reqfields[2]
            else:
                self.protocol = "unknown"
        u = urlparse(self.url)
        self.hostname = u.hostname
        self.path = u.path 
        self.useragent = fields[13]
        self.encryption = fields[14]
        self.encryptionprotocol = fields[15]
        self.targetgroup = fields[16]
        self.traceid = fields[17]

class CfLogLine:
    """CfLogLine"""

    @staticmethod
    def get_delimiter():
        return '\t'

    @staticmethod
    def get_gzipped():
        return True

    @staticmethod
    def get_isdataline():
        return lambda row: not row[0].startswith('#')

    def __init__(self, fields):
        self.utctime = datetime.strptime(fields[0] + "T" + fields[1], '%Y-%m-%dT%H:%M:%S')
        self.edge_location = fields[2]
        self.responsebytes = fields[3]
        self.remoteip = fields[4]
        self.method = fields[5]
        self.server = fields[6]
        self.path = fields[7]
        self.responsecode = int(fields[8])
        self.referrer = fields[9]
        self.useragent = fields[10]
        self.querystring = fields[11]
        self.cookie = fields[12]
        self.edge_resulttype = fields[13]
        self.requestid = fields[14]
        self.hostname = fields[15]
        self.protocol = fields[16]
        self.request_bytes = int(fields[17])
        self.timetaken = float(fields[18])
        # self.forwardedfor = fields[19]
        # self.sslprotocol = fields[20]
        # self.sslcipher = fields[21]
        self.edge_responseresulttype = fields[22]
        self.protocol_version = fields[23]


