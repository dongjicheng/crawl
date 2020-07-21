# -*- coding: utf-8 -*-
import json
import time
import pymongo
import hashlib
import urllib
import urllib2
import StringIO
import random
import pycurl
import os
import re
import sys
import traceback
import logging
import socket
import Queue
import StringIO
import threading
from mitmproxy import flow, proxy, controller, options
from mitmproxy.proxy.server import ProxyServer
from mitmproxy.models.http import HTTPResponse
from netlib.http import Headers
import RoomRankingStarRequest
import UserCardLiteRequest

logger = logging.getLogger('immomo')
taskQ = Queue.Queue()

class immomo(flow.FlowMaster):
    def __init__(self, opts, server, state):
        super(immomo, self).__init__(opts, server, state)

    def run(self):
        try:
            logging.info("proxy server started successfully...")
            flow.FlowMaster.run(self)
        except KeyboardInterrupt:
            self.shutdown()
            logging.info("Ctrl C - stopping immomo server")

    @controller.handler
    def request(self, f):
        #if f.request.host == "live-api.immomo.com":
        #   self.reverse(f)
        if f.request.host <> "192.168.2.150":
            print f.request.host
            pass
        elif f.request.host == "192.168.2.150" and f.request.method == "GET":
            self.do_GET(f)
        elif f.request.host == "192.168.2.150" and f.request.method == "POST":
            self.do_POST(f)

        pass

    @controller.handler
    def response(self, f):
        pass

    def do_GET(self, f):
        try:
            data = taskQ.get(timeout=0)
        except:
            data = {}
            print '{0}, no task'.format(time.strftime('%H:%M:%S'))

        headers = Headers()
        headers["Content-Type"] = "application/json;charset=UTF-8"
        f.response = HTTPResponse(b"HTTP/1.1",200,"OK",headers,json.dumps(data))

    def do_POST(self, f):
        content = f.request.get_text()
        RoomRankingStarRequest.saveResponse(content)
        headers = Headers()
        f.response = HTTPResponse(b"HTTP/1.1",200,"OK",headers,"")
        
    def reverse(self, f):
        headers = map(lambda x:'{}:{}'.format(x[0],x[1]), f.request.headers.fields)
        body = StringIO.StringIO()
        header = StringIO.StringIO()
        
        c = pycurl.Curl()
        c.setopt(pycurl.TIMEOUT, 5)
        c.setopt(pycurl.URL, f.request.url)
        c.setopt(pycurl.HTTPHEADER, headers)
        c.setopt(pycurl.SSL_VERIFYPEER,0)
        c.setopt(pycurl.SSL_VERIFYHOST,0)
        c.setopt(pycurl.FOLLOWLOCATION, False)
        if f.request.method.upper() == "POST":
            c.setopt(pycurl.POST,1)
        if f.request.raw_content:
            c.setopt(pycurl.POSTFIELDS, f.request.raw_content)
        c.setopt(pycurl.WRITEHEADER, header)
        c.setopt(pycurl.WRITEDATA, body)
        #c.setopt(pycurl.PROXY, 'http://127.0.0.1:8888')
        c.setopt(pycurl.PROXY, 'http://192.168.0.71:3128')
        c.setopt(pycurl.PROXYUSERPWD, "u1:crawl")
        try:
            c.perform()
        except pycurl.error,err:
            msg = '{0}, {1}'.format(err[0],err[1])
            logger.error(msg)
        #finally:
        #    c.close()

        fields = []
        for line in header.getvalue().split("\r\n"):
            if line.startswith("HTTP"):
                http_version, status_code, reason = line.split(b" ", 2)
                fields = []
                continue
            if ':' in line:
                name, value = line.split(b": ", 1)
                fields.append((name, value))
        headers = Headers()
        headers.fields = tuple(fields)
        f.response = HTTPResponse(http_version,int(status_code),reason,headers,body.getvalue())
        c.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,  
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',  
                        filename='immomo.log',
                        filemode='w')
    console = logging.StreamHandler() 
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    taskQ.put(UserCardLiteRequest.data)
    #taskQ.put(RoomRankingStarRequest.getRequest())
    #threading.Thread(target=RoomRankingStarRequest.run,args=(taskQ,)).start()

    opts = options.Options(listen_port=8080,mode='regular',cadir="./ssl/")
    config = proxy.ProxyConfig(opts)
    state = flow.State()
    server = ProxyServer(config)
    m = immomo(opts, server, state)
    m.run()
