# -*- coding: utf-8 -*-
import pymongo
import Queue
import time
import sys
import random
import threading
import multiprocessing
from BaseApiRequeset import BaseApiRequeset
from WorkProcess import WorkProcess, ResponseError, NilError
from Monitor import ThreadMonitor

class UserCardLiteRequest(BaseApiRequeset):
    TASKS = multiprocessing.Queue()
    Collection = ""
    def __init__(self,proxy,userpwd,db,lock,roomids):
        super(UserCardLiteRequest,self).__init__(proxy,userpwd,db,lock)
        self.collection = UserCardLiteRequest.Collection
        self.roomids = roomids

    def createRequest(self):
        with self.lock:
            if UserCardLiteRequest.TASKS.empty():
                raise NilError()
            position,remoteid,roomid = UserCardLiteRequest.TASKS.get()
        sourceid = roomid
        if not roomid:
            roomid = random.choice(self.roomids)
            
        return {
                    "url":"https://live-api.immomo.com/guestv3/user/card/lite",
                    "position":position,
                    "roomid":sourceid,
                    "formdata":{
                        "remoteid":remoteid,
                        "roomid":roomid
                    }
                }
        
    def getRequest(self):
        return self.createRequest()

    def onResponse(self, request, response):
        pass
            
    def onError(self, request, response):
        pass

class UserCardLiteProcess(WorkProcess):
    def __init__(self,proxies,lock,status,roomids):
        proxies = multiprocessing.Manager().list(proxies)
        self.roomids = roomids
        super(UserCardLiteProcess,self).__init__(proxies,lock,status)
    def initRequest(self, proxy, userpwd, db):
        return UserCardLiteRequest(proxy, userpwd, db, self.lock, self.roomids)

if __name__ == "__main__":
    lock = multiprocessing.Lock()
    UserCardLiteRequest.TASKS.put((1,"337153000","14545548265"))
    handle = UserCardLiteRequest("http://127.0.0.1:8888", "", None, lock, None)
    request = handle.getRequest()
    handle.onRequest(request)
    response = handle.download(request)
    handle.onResponse(request,response)
