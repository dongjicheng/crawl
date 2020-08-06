#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 2019-10-25 companyids 未更新！
import json
import time
import pymongo
import hashlib
import Queue
import threading
import urllib2
import random
import pycurl
import os
import urllib
import traceback
import StringIO
import re
import sys
import lxml
from lxml import etree

maxCount = 0
queue = Queue.Queue()
# db = pymongo.MongoClient('mongodb://192.168.0.13')['liepin']
db = pymongo.MongoClient('mongodb://localhost')['liepin']
jobCol = time.strftime('job_%Y%m'
                       '%d', time.localtime(time.time()))
headers = [
    "User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "X-Requested-With: XMLHttpRequest"]


def reader():
    global queue
    # ids = db["companyIds"].distinct("_id")
    ids = db['CompanyIdLib_20200617'].distinct('_id')
    url = "https://www.liepin.com/company/sojob.json"
    for i, id in enumerate(ids):
        if i % 10000 == 0:
            print
            time.strftime('%Y-%m-%d %H:%M:%S Loading...'), i
        formdata = {
            "ecompIds": id,
            "pageSize": "15",
            "curPage": 0,
            "keywords": "",
            "dq": "",
            "deptId": "",
        }
        task = {"url": url, "formdata": formdata}
        queue.put(task)
    maxCount = queue.qsize()
    print
    time.strftime('%Y-%m-%d %H:%M:%S Loading complete'), maxCount


class ThreadUrl(threading.Thread):
    def __init__(self, ip):
        threading.Thread.__init__(self)
        self.ip = ip

    def run(self):
        global queue, db, headers
        while True:
            request = queue.get()
            try:
                end = time.time() + 1.0

                c = pycurl.Curl()
                body = StringIO.StringIO()
                c.setopt(pycurl.TIMEOUT, 60)
                c.setopt(pycurl.URL, request["url"])
                c.setopt(pycurl.HTTPHEADER, headers)
                c.setopt(pycurl.ENCODING, 'gzip,deflate')
                c.setopt(pycurl.WRITEFUNCTION, body.write)
                # c.setopt(pycurl.PROXY, "http://127.0.0.1:8888")                #Fiddler
                c.setopt(pycurl.SSL_VERIFYPEER, 0)
                c.setopt(pycurl.SSL_VERIFYHOST, 0)
                # c.setopt(pycurl.INTERFACE, self.ip)
                if 'formdata' in request:
                    postfields = urllib.urlencode(request['formdata'])
                    c.setopt(pycurl.POST, 1)
                    c.setopt(pycurl.POSTFIELDS, postfields)
                if 'postfields' in request:
                    c.setopt(pycurl.POST, 1)
                    c.setopt(pycurl.POSTFIELDS, request['postfields'])

                begin = time.time()
                try:
                    c.perform()
                except pycurl.error, err:
                    print
                    err[0], err[1]
                    queue.put(request)
                    continue
                finally:
                    c.close()

                data = json.loads(body.getvalue())
                data["formdata"] = request["formdata"]
                db[jobCol].save(data)
                if data["data"]["curPage"] + 1 <= data["data"]["totalPage"]:
                    request["formdata"]["curPage"] = data["data"]["curPage"] + 1
                    queue.put(request)

            except pycurl.error, err:
                queue.put(request)
                print
                err
                threading._sleep(random.randint(10, 30))
            except Exception, ex:
                print
                traceback.format_exc()
            finally:
                if time.time() < end:
                    threading._sleep(end - time.time())
                queue.task_done()


class ThreadMonitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        prevTime = 0
        prevCount = maxCount
        while True:
            dt = time.time() - prevTime
            end = time.time() + 30

            print
            '%s Download %d urls (at %.2f url/min), Queue %d tasks' % \
            (time.strftime('%Y-%m-%d %H:%M:%S'), maxCount - queue.qsize(), 60 * (prevCount - queue.qsize()) / dt,
             queue.qsize())
            prevCount = queue.qsize()
            prevTime = time.time()

            if time.time() < end:
                threading._sleep(end - time.time())


if __name__ == '__main__':
    reader()
    addr = map(lambda x: '10.0.%d.1' % x, range(0, 27))

    for ip in addr:
        t = ThreadUrl(ip)
        t.setDaemon(True)  # 设置为守护线程
        t.start()
        # break

    t = ThreadMonitor()  # 监控线程
    t.setDaemon(True)
    t.start()
    queue.join()