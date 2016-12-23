import boto3
import os
import StringIO
from datetime import tzinfo, datetime, timedelta
import csv
import pandas as pd
import numpy as np
from urlparse import urlparse
import hashlib
import urllib2
import json

ZERO = timedelta(0)


#A class to fetch a list of logfilesd from S3 object summaries and put them into a dataframe
class LogDataFrame:
    """LogDataFrame"""

    def __init__(self, s3res ):
        self.s3res = s3res

    # Maybe add an argument to exclude lines. Perhaps a lambda that takes 
    # a LogLine object and returns a bool or something to scan a string and return a bool
    def make_dataframe(self, s3items, loglinefilter=None):
        loglines = []
        for s3objsummary in s3items:
            s3obj = self.s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
            buf = StringIO.StringIO()
            s3obj.download_fileobj(buf)
            buf.seek(0)
            self.append_lines(buf, loglines, loglinefilter)
            buf.close()
        return self.get_df_from_loglines(loglines)

    def make_dataframe_fromfolder(self, foldername, loglinefilter=None):
        fullfoldername = os.path.expanduser(foldername)
        if os.path.exists(fullfoldername):
            loglines = []
            filelist = [f for f in os.listdir(fullfoldername) if os.path.isfile(os.path.join(fullfoldername, f))]
            for filename in filelist:
                with open(os.path.join(fullfoldername, filename), 'rb') as buf:
                    self.append_lines(buf, loglines, loglinefilter)
            return self.get_df_from_loglines(loglines)

    def append_lines(self, f, loglines, loglinefilter):
        csvreader = csv.reader(f, delimiter = ' ', quotechar = '"')
        for row in csvreader:
            l = LogLine(row)
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
            bucket = "123logging", minimumfiles = 5, strictreftime = False):
        self.account = account if account != None else self.get_awsacctno()
        self.region = region
        self.minimumfiles = minimumfiles
        self.s3res = s3res
        self.bucket = bucket
        self.strictreftime = strictreftime

    def get_recents(self, lbname, refdate=None, lblogfolder = None):
        utc = UTC()
        allitems = []
        checkedkeys = set()
        iterations = 0
        maxiterations = 500
        tenminspast = timedelta(minutes=-10)
        starttime = refdate if refdate != None else datetime.now(utc) 
        logfolder = lblogfolder if lblogfolder != None else lbname
        mytime = starttime
        s3foldertemplate = "loadbalancers/{loadbalancer}/AWSLogs/{account}/elasticloadbalancing/{region}/{dt.year:0>4}/{dt.month:0>2}/{dt.day:0>2}/"
        s3filekeyroottemplate = "{account}_elasticloadbalancing_{region}_{loadbalancer}_{dt.year:0>4}{dt.month:0>2}{dt.day:0>2}T{dt.hour:0>2}"
        while (len(allitems) <= self.minimumfiles and iterations < maxiterations):
            folderprefix = s3foldertemplate.format(dt = mytime, loadbalancer = logfolder, account = self.account, region = self.region) 
            itemprefix = s3filekeyroottemplate.format(dt = mytime, loadbalancer = lbname, account = self.account, region = self.region )
            fullprefix = folderprefix + itemprefix
            if (fullprefix not in checkedkeys):
                print fullprefix
                bucket = self.s3res.Bucket(self.bucket)
                allitems.extend( filter( lambda item: (self.strictreftime == False) or (refdate == None) or (item.last_modified < refdate), sorted( bucket.objects.filter(Prefix=folderprefix + itemprefix), key = lambda item: item.last_modified, reverse=True)))
                checkedkeys.add(fullprefix)
            iterations += 1
            mytime += tenminspast
            

        recents = [x for ind, x in enumerate(allitems) if self.minimumfiles > ind >= 0 ]
        return recents

    def get_awsacctno(self):
        metadata = json.loads(urllib2.urlopen('http://169.254.169.254/latest/meta-data/iam/info/').read())
        arn = metadata['InstanceProfileArn']
        elts = arn.split(':')
        acctno =  elts[4]
        return acctno


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
        self.responsecode = int(fields[7])
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



