# -*- coding: utf-8 -*-
import pymongo
import multiprocessing
import Queue
import pycurl
import random
import time
import threading
import traceback

class ResponseError(Exception):
    pass
class NilError(Exception):
    pass

#class WorkProcess(threading.Thread):
class WorkProcess(multiprocessing.Process):
    status = None
    complete = None
    db = pymongo.MongoClient('mongodb://192.168.0.13')['immomo']

    def __init__(self, proxies, cls):
        super(WorkProcess,self).__init__()
        self.daemon = True
        random.shuffle(proxies)
        self.requests = map(lambda proxy:cls(proxy[0],proxy[1]),proxies)

    @staticmethod
    def getDB():
        # Each process creates its own instance of MongoClient.
        return pymongo.MongoClient('mongodb://192.168.0.13')['immomo']

    def run(self):
        count = 0
        handle = None
        request = None
        db = WorkProcess.getDB()
        while True:
            try:
                if not handle:
                    count = 0
                    handle = self.requests.pop(0)
                if not request:
                    request = handle.getRequest()
                handle.onRequest(request)
                response = handle.download(request)
                handle.onResponse(request,response,db)
                request = None
            except pycurl.error, e:
                print '{}, {}, {}, {}, {}'.format(time.strftime('%H:%M:%S'), 
                                    handle.proxy, handle.userpwd, 
                                    e[0], e[1])
                continue
            except ResponseError, e:
                print '{}, {}, {}, {}, {}'.format(time.strftime('%H:%M:%S'), 
                                    handle.proxy, handle.userpwd, 
                                    e[0], e[1])
                request = handle.onError(request,response,db)
                continue
            except NilError, e:
                break
            except Exception, e:
                print '{}, {}, {}, {}'.format(time.strftime('%H:%M:%S'), 
                                    handle.proxy, handle.userpwd, e)
                continue
            finally:
                if not request:
                    WorkProcess.status[handle.getProxy()] = True
                    count += 1
                    if count >= 10:
                        self.requests.append(handle)
                        handle = None
                else:
                    WorkProcess.status[handle.getProxy()] = False
                    self.requests.append(handle)
                    handle = None
                WorkProcess.complete.value += 1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

