# -*- coding: utf-8 -*-
import pymongo
import sys
import os
import re
import time
import Queue
import json
import time
import random
import ctypes
import HttpsProxy
import multiprocessing
from bisect import bisect_left
from datetime import datetime,timedelta
from BaseApiRequeset import BaseApiRequeset
from WorkProcess import WorkProcess, ResponseError, NilError
from Monitor import ThreadMonitor

class RankingCardRequest(BaseApiRequeset):
    Ids = None
    TaskCount = None
    CardQ = multiprocessing.Queue()
    RankQ = multiprocessing.Queue()
    Single = multiprocessing.Event()

    def __init__(self,proxy,userpwd):
        super(RankingCardRequest,self).__init__(proxy,userpwd)
        day = datetime.now() - timedelta(datetime.now().weekday())
        self.card = day.strftime('ranking_card_%Y%m%d')
        self.rank = day.strftime('totalstars_%Y%m%d')

    def createRequest(self):
        RankingCardRequest.TaskCount.value = RankingCardRequest.CardQ.qsize() + RankingCardRequest.RankQ.qsize()

        if not RankingCardRequest.CardQ.empty():
            remoteid,roomid = RankingCardRequest.CardQ.get()
            return {
                    "url":"https://live-api.immomo.com/guestv3/user/card/lite",
                    "type":"card",
                    "formdata":{
                        "remoteid":remoteid,
                        "roomid":roomid
                    }
                }
        elif not RankingCardRequest.RankQ.empty():
            starid,roomid = RankingCardRequest.RankQ.get()
            return {
                        "url": "https://live-api.immomo.com/guestv3/room/ranking/totalstars",
                        "type":"rank",
                        "formdata":{
                            "starid": starid,
                            "roomid": roomid,
                            }
                        }
        else:
            RankingCardRequest.Single.set()
            raise NilError()
    def getRequest(self):
        while True:
            try:
                return self.createRequest()
            except:
                if RankingCardRequest.Single.wait(1):
                    raise NilError()

    def onResponse(self, request, response, db):
        if response['ec'] <> 0:
            raise ResponseError(response['ec'], response['em'].encode('utf-8'))
        if request["type"] == "rank":
            for data in (response['data']['below_ranks'] + response['data']['above_ranks']):
                if data['momoid'] not in RankingCardRequest.Ids and data['charm'] >= 7:
                    RankingCardRequest.CardQ.put((data['momoid'],random.choice(RankingCardRequest.Ids.values())))
            db[self.rank].save(response)
        elif request["type"] == "card":
            try:
                momoid = response['data']['momoid']
                roomid = response['data']['room_goto'].split('|')[2]
                if momoid not in RankingCardRequest.Ids or \
                    roomid <> RankingCardRequest.Ids[momoid]:
                    data = { 
                            "_id" : {
                                "momoid" : momoid, 
                                "roomid" : roomid
                            }, 
                            "charm" : response['data']['charm'], 
                            "nick" : response['data']['nick'], 
                            "time" : datetime.now().strftime("%Y-%m-%d"),
                        }
                    RankingCardRequest.Ids.setdefault(momoid, roomid)
                    RankingCardRequest.RankQ.put((momoid,roomid))
                    db["roomids"].save(data)
                    pass
            except:
                pass
            db[self.card].save(response)
        super(RankingCardRequest,self).onResponse(request, response, db)


    def onError(self, request, response, db):
        #print '{}, onError, {}, {}, {}'.format(time.strftime('%H:%M:%S'),
        #                            self.proxy,
        #                            response['ec'],
        #                            response['em'].encode('utf-8'))

        if response['ec'] == 400:
            print request['formdata']
            return None

        time.sleep(10)
        self.cookie = ""
        return request

if __name__ == "__main__":
    # kill old process
    cur = os.getpid()
    cmd = "ps -ef|grep %s|grep -v grep|awk '{print $2}'" % sys.argv[0]
    for pid in os.popen(cmd):
        if int(pid) != int(cur):
            os.system("kill -9 %s" % pid)

    manager = multiprocessing.Manager()

    WorkProcess.lock = multiprocessing.Lock()
    WorkProcess.status = manager.dict()
    WorkProcess.complete = multiprocessing.Value(ctypes.c_int,0)

    pipeline = [
                {
                    "$match": {
                        "_id.roomid": {
                            "$ne": None
                        }
                    }
                }, 
                {
                    "$group": {
                        "_id": "$_id.momoid", 
                        "roomid": {
                            "$last": "$_id.roomid"
                        }, 
                        "charm": {
                            "$last": "$charm"
                        }
                    }
                }, 
                {
                    "$sort": {
                        "charm": -1
                    }
                }
            ]

    ids = map(lambda x:(x["_id"],x["roomid"]),
              WorkProcess.getDB()["roomids"].aggregate(pipeline,allowDiskUse=True))

    # 任务数量
    RankingCardRequest.TaskCount = manager.Value(ctypes.c_int, 0)
    
    # 顺序（charm）card
    RankingCardRequest.CardQ = multiprocessing.Queue()
    map(lambda x:RankingCardRequest.CardQ.put(x),ids)

    # 随机rank
    random.shuffle(ids)
    RankingCardRequest.RankQ = multiprocessing.Queue()
    map(lambda x:RankingCardRequest.RankQ.put(x),ids)

    # ids
    RankingCardRequest.Ids = manager.dict()
    map(lambda x:RankingCardRequest.Ids.setdefault(x[0],x[1]),ids)

    tMonitor = ThreadMonitor(RankingCardRequest.TaskCount)
    tMonitor.start()

    threads = []
    proxies = HttpsProxy.getProxy()
    for i in range(10):
        t = WorkProcess(proxies[i::10], RankingCardRequest)
        t.start()
        threads.append(t)
    map(lambda t:t.join(), threads)
    
    #t = WorkProcess([("http://127.0.0.1:8888","")], RankingCardRequest)
    #t.start()
    #t.join()
