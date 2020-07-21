# -*- coding: utf-8 -*-
import pymongo
import sys
import re
import time
import Queue
import json
import time
import HttpsProxy
import multiprocessing
from BaseApiRequeset import BaseApiRequeset
from WorkProcess import WorkProcess, ResponseError, NilError
from Monitor import ThreadMonitor

class RoomRankingTotalStarsRequest(BaseApiRequeset):
    TASKS = multiprocessing.Queue()
    def __init__(self,proxy,userpwd,db,lock,position):
        super(RoomRankingTotalStarsRequest,self).__init__(proxy,userpwd,db,lock)
        self.collection = 'totalstars_' + sys.argv[1]
        self.position = position

    def createRequest(self, starid, position):
        with self.lock:
            self.position.value = position
        if position == 1:
            raise NilError()
        return {
                    "url": "https://live-api.immomo.com/guestv3/room/ranking/totalstars",
                    "position": position,
                    "formdata":{
                        "starid": starid,
                        "roomid": str(int((time.time()*10))),
                        }
                    }
    def getRequest(self):
        if self.TASKS.empty():
            items = self.db[self.collection].find({'data.star_rank.momoid':{'$exists':True}}).sort('data.star_rank.position',pymongo.ASCENDING).limit(1)
            #items = db[self.collection].find({'data.below_ranks':{'$ne':[]}}).sort('data.star_rank.position',pymongo.DESCENDING).limit(1)
            momoid = 100130
            position = 3198637
            for item in items:
                momoid = item['data']['star_rank'][0]['momoid']
                position = item['data']['star_rank'][0]['position']
            RoomRankingTotalStarsRequest.TASKS.put((momoid,position))
        momoid,position = RoomRankingTotalStarsRequest.TASKS.get()
        return self.createRequest(momoid,position)

    @staticmethod
    def check(totalstars):
        db = WorkProcess.getDB()
        collection = totalstars + "_check"
        db[collection].drop()
        items = db[totalstars].find()
        for i,item in enumerate(items):
            users = []
            users.extend(item['data']['star_rank'])
            users.extend(item['data']['below_ranks'])
            users.extend(item['data']['above_ranks'])
            users = sorted(users,key=lambda x:x['position'])
            charm = users[0]['charm']
            star_charm = item['data']['star_rank'][0]['charm']
            for user in users[1:]:
                if abs(user['charm'] - star_charm) > (star_charm/5) + 1:
                    db[coltction].save(user)
                else:
                    charm = user['charm']
            if i % 10000 == 0:
                print '{} load {} {}w'.format(time.strftime('%H:%M:%S'),coltction, i/10000)
    @staticmethod
    def __clean(users,results):
        for user in users:
            if user['momoid'] not in results:
                results[user['momoid']] = {
                    '_id':user['momoid'],
                    'position':user["position"]
                }
            else:
                results[user['momoid']]["position"] = user["position"]
    @staticmethod
    def clean(prev, rank):
        prev = "totalstars_clean_" + prev
        collection = "totalstars_clean_" + rank
        rank = "totalstars_" + rank

        db = WorkProcess.getDB()
        results = {}
        items = db[prev].find()
        for i,item in enumerate(items):
            results[item['_id']] = item
            if i % 100000 == 0:
                print '{} load {} {}w'.format(time.strftime('%H:%M:%S'),prev, i/10000)

        items = db[rank].find({},{
                'data.star_rank.momoid':1,
                'data.star_rank.position':1,
                'data.below_ranks.momoid':1,
                'data.below_ranks.position':1,
                'data.above_ranks.momoid':1,
                'data.above_ranks.position':1,
            })
        for i,item in enumerate(items):
            try:
                if 'momoid' in item['data']['star_rank'][0]:
                    RoomRankingTotalStarsRequest.__clean(item['data']['star_rank'], results)
                if 'below_ranks' in item['data'] and item['data']['below_ranks']:
                    RoomRankingTotalStarsRequest.__clean(item['data']['below_ranks'], results)
                if 'above_ranks' in item['data'] and item['data']['above_ranks']:
                    RoomRankingTotalStarsRequest.__clean(item['data']['above_ranks'], results)
            except Exception,e:
                print item['_id']
                raise e
            if i % 50000 == 0:
                print '{} load {} {}w'.format(time.strftime('%H:%M:%S'),rank, i/10000)

        values = results.values()
        db[collection].drop()
        for i in range(0, len(values), 1000):
            while True:
                try:
                    db[collection].insert(values[i:i+1000])
                    if i % 50000 == 0:
                        print '{} save {} {}w'.format(time.strftime('%H:%M:%S'), collection, i/10000)
                    break
                except e:
                    print e
                    time.sleep(10)

    def onResponse(self, request, response):
        if response['ec'] == 0:
            super(RoomRankingTotalStarsRequest,self).onResponse(request, response)
            #print '{0}, {1}, {2}'.format(time.strftime('%H:%M:%S'),
            #                            response['data']['star_rank'][0]['position'],
            #                            response['data']['star_rank'][0]['nickname'].encode('utf-8'))
            if  "momoid" not in response["data"]["star_rank"][0]:
                raise ResponseError(0, "data.star_rank.momoid is null")
            if abs(response["data"]["star_rank"][0]["position"] - request["position"]) > 10000:
                raise ResponseError(0, "position: {}-{}".format(response["data"]["star_rank"][0]["position"], request["position"]))
            RoomRankingTotalStarsRequest.TASKS.put((response["data"]["above_ranks"][0]["momoid"],response["data"]["above_ranks"][0]["position"]))
            if not response["data"]["below_ranks"]:
                raise ResponseError(0, "below_ranks or momoid is null")
            #RoomRankingTotalStarsRequest.TASK = (response["data"]["below_ranks"][-1]["momoid"],response["data"]["below_ranks"][-1]["position"])
        else:
            print "onResponse", response
            raise ResponseError(response['ec'], response['em'])

    def onError(self, request, response):
        print '{}, onError, {}, {}, {}'.format(time.strftime('%H:%M:%S'),
                                    self.proxy,
                                    response['ec'],
                                    response['em'].encode('utf-8'))

        time.sleep(10)
        self.cookie = ""
        return None

class RoomRankingTotalStarsRequestProcess(WorkProcess): 
    def __init__(self,proxies,lock,status,position):
        self.position = position
        proxies = multiprocessing.Manager().list(proxies)
        super(RoomRankingTotalStarsRequestProcess,self).__init__(proxies,lock,status)
    def initRequest(self, proxy, userpwd, db):
        return RoomRankingTotalStarsRequest(proxy, userpwd, db, self.lock, self.position)

if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "clean":
        RoomRankingTotalStarsRequest.clean(sys.argv[2], sys.argv[3])
    else:
        status = multiprocessing.Manager().dict()
        lock = multiprocessing.Lock()
        position = multiprocessing.Value('I',0)
        t = RoomRankingTotalStarsRequestProcess(HttpsProxy.getProxy(),lock,status,position)
        #t = RoomRankingTotalStarsRequestProcess([("http://127.0.0.1:8888","")],lock,status,position)
        t.start()
    
        while True:
            with lock:
                if position.value > 0:
                    break
            time.sleep(1)

        tMonitor = ThreadMonitor(lock,status,position)
        tMonitor.start()

        t.join()
    ##RoomRankingTotalStarsRequest.check('totalstars_20171201')
    
    #RoomRankingTotalStarsRequest.clean(sys.argv[1], sys.argv[2])
