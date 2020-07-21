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

    @staticmethod
    def load(collection,roomids):
        ids = set()
        items = WorkProcess.getDB()[UserCardLiteRequest.Collection].find({},{"_id":True})
        map(lambda item:ids.add(item["_id"]), items)
        print "load {} {}".format(UserCardLiteRequest.Collection,len(ids))

        items = WorkProcess.getDB()[collection].find()
        
        tasks = []
        for i,item in enumerate(items):
            if i % 100000 == 0:
                print '{} load {} {}w'.format(time.strftime('%H:%M:%S'),collection, i/10000)

            if "roomid" in item and item['roomid']:
                roomids.append(item['roomid'])

            if item['_id'] in ids:
                continue
            
            tasks.append((item['charm'],item['_id'],item['roomid']))
            
            #if i == 10000:
            #    break
        #tasks.sort()
        map(lambda x:UserCardLiteRequest.TASKS.put(x), tasks)
        print '{} load {} {}'.format(time.strftime('%H:%M:%S'),collection, UserCardLiteRequest.TASKS.qsize())

    @staticmethod
    def clean( roominfo, usercard, collection):
        print '{} load {} ......'.format(time.strftime('%H:%M:%S'),collection)
        items = WorkProcess.getDB()[roominfo].find(pipeline)
        roominfos = {item['_id']:item['roomid'] for item in items}
        print '{} load {} {}'.format(time.strftime('%H:%M:%S'),collection,len(roominfos))

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
        if response['ec'] == 0:
            response['_id'] = response['data']['momoid']
            response['roomid'] = request['roomid']
            response['remoteid'] = request['formdata']['remoteid']
            super(UserCardLiteRequest,self).onResponse(request, response)
            #print '{0}, {1}, {2}'.format(time.strftime('%H:%M:%S'),
            #                            request['position'],
            #                            response['data']['nick'].encode('utf-8'))
        else:
            raise ResponseError(response['ec'], response['em'])
            
    def onError(self, request, response):
        print '{}, onError, {}, {}, {}'.format(time.strftime('%H:%M:%S'),
                                    self.proxy,
                                    response['ec'],
                                    response['em'].encode('utf-8'))
        if response['ec'] == 400:
            print request['formdata']
            return None
            
        time.sleep(10)
        self.cookie = ""
        return request

class UserCardLiteProcess(WorkProcess):
    def __init__(self,proxies,lock,status,roomids):
        proxies = multiprocessing.Manager().list(proxies)
        self.roomids = roomids
        super(UserCardLiteProcess,self).__init__(proxies,lock,status)
    def initRequest(self, proxy, userpwd, db):
        return UserCardLiteRequest(proxy, userpwd, db, self.lock, self.roomids)

if __name__ == "__main__":
    manager = multiprocessing.Manager()
    status = manager.dict()
    lock = multiprocessing.Lock()
    roomids = manager.list()
    UserCardLiteRequest.Collection = "user_card_lite_" + sys.argv[1]
    UserCardLiteRequest.load("roomids_20181006_sort", roomids)

    t = ThreadMonitor(lock, status, UserCardLiteRequest.TASKS)
    t.start()

    threads = []
    for ip in range(71,79):
        if ip == 71:
            proxies = map(lambda x:("http://192.168.0.71:3128","u{}:crawl".format(x)), range(28))
        else:
            proxies = map(lambda x:("http://192.168.0.{}:3128".format(ip),"u{}:crawl".format(x)), range(30))
        
        t = UserCardLiteProcess(proxies, lock, status, roomids)
        t.start()
        threads.append(t)

    map(lambda t:t.join(), threads)

    ##proxies = [("http://127.0.0.1:8888","")]
    ##UserCardLiteRequest.TASKS.put((1,"337153000","1479444878199"))
    ##t = UserCardLiteProcess(proxies, lock, status, [""])
    ##t.start()
    ##t.join()
