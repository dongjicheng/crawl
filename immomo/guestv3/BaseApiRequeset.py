# -*- coding: utf-8 -*-
import json
import time
import Queue
import pycurl
import pymongo
import threading
import urllib
import random
import StringIO
import GuestLogin
import HttpsProxy
import multiprocessing
import traceback

class BaseApiRequeset(object):
    def __init__(self,proxy,userpwd):
        self.proxy = proxy
        self.userpwd = userpwd

        self.cookie = ""
        self.count = 0
        self.maxCount = 50

    def getProxy(self):
        return (self.proxy,self.userpwd)

    def getHeaders(self):
        headers =  [
                    'User-Agent: MomoChat/8.9.5 Android/3147 (SM-G9006W; Android 6.0.1; Gapps 0; zh_CN; 1; samsung)',
                    "Content-Type: application/x-www-form-urlencoded",
                ]
        if self.cookie:
            headers.append('cookie:' + self.cookie)

        return headers

    def onRequest(self, request):
        while not self.cookie:
            try:
                data = self.download(GuestLogin.getSessionID())
                if "session" in data['data']:
                    self.cookie = 'SESSIONID=' + data['data']["session"]
                else:
                    raise Exception("Session null")
            except pycurl.error,err:
                raise err
            except Exception,ex:
                #print traceback.print_exc()
                raise ex

    def onResponse(self, request, response, db):
        self.count += 1
        if self.count >= self.maxCount:
            self.count = 0
            self.cookie = ''
        pass

    def onError(self, request, response, db):
        pass

    def getRequest(self):
        pass

    def download(self,request):
        headers = self.getHeaders()
        formdata = urllib.urlencode(request['formdata'])
        c = pycurl.Curl()
        body = StringIO.StringIO()
        c.setopt(pycurl.TIMEOUT, 3)
        c.setopt(pycurl.CONNECTTIMEOUT, 1) 
        c.setopt(pycurl.URL, request['url'])
        c.setopt(pycurl.HTTPHEADER, headers)
        c.setopt(pycurl.ENCODING, 'gzip,deflate')
        c.setopt(pycurl.SSL_VERIFYPEER,0)
        c.setopt(pycurl.SSL_VERIFYHOST,0)
        c.setopt(pycurl.POST,1)
        c.setopt(pycurl.POSTFIELDS,formdata)
        c.setopt(pycurl.WRITEFUNCTION, body.write)
        c.setopt(pycurl.PROXY, self.proxy)
        c.setopt(pycurl.PROXYUSERPWD, self.userpwd)

        try:
            c.perform()
            code = c.getinfo(pycurl.RESPONSE_CODE)
            if code <> 200:
                raise pycurl.error(code, "")
        except pycurl.error,err:
            #print traceback.print_exc() 
            if err[0] not in (7,28,56):
                print '{}, {}, {}, {}, {}'.format(time.strftime('%H:%M:%S'), 
                                                  self.proxy, self.userpwd, 
                                                  err[0], err[1])
            raise err
        finally:
            c.close()
        
        return json.loads(body.getvalue())

