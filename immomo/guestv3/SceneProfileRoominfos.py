# -*- coding: utf-8 -*-
import sys
import time
import threading
import HttpsProxy
import multiprocessing
from BaseApiRequeset import BaseApiRequeset
from WorkProcess import WorkProcess, ResponseError, NilError
from Monitor import ThreadMonitor

class SceneProfileRoominfos(BaseApiRequeset):
    TASKS = multiprocessing.Queue()
    def __init__(self, proxy, userpwd, db, lock):
        super(SceneProfileRoominfos,self).__init__(proxy, userpwd, db, lock)
        self.collection = time.strftime('roominfos_%Y%m%d',time.localtime(time.time()))
        #self.collection = "roominfos_20171213"
        self.maxCount = 20

    @staticmethod
    def load(collection):
        print '{} load {} ......'.format(time.strftime('%H:%M:%S'),collection)
        pipeline = [
                        {
                            "$match": {
                                "roomid": {
                                    "$exists": False
                                }
                            }
                        }, 
                        {
                            "$lookup": {
                                "from": "roominfos_20171213", 
                                "localField": "_id", 
                                "foreignField": "_id", 
                                "as": "roominfos"
                            }
                        }, 
                        {
                            "$match": {
                                "roominfos": [ ]
                            }
                        }
                    ]
        items = WorkProcess.getDB()[collection].aggregate(pipeline)
        map(lambda id:SceneProfileRoominfos.TASKS.put(id["_id"]), items)

        print '{} load {} {}'.format(time.strftime('%H:%M:%S'),collection,SceneProfileRoominfos.TASKS.qsize())

    def getHeaders(self):
        return  [
                    'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
                    "Content-Type: application/x-www-form-urlencoded",
                    "Referer: https://web.immomo.com",
                    "Origin: https://web.immomo.com",
                    "X-Requested-With: XMLHttpRequest",
                    "Cookie:" + self.cookie
                ]
    def createRequest(self):
        with self.lock:
            if SceneProfileRoominfos.TASKS.empty():
                raise NilError()
            stid = SceneProfileRoominfos.TASKS.get()
        return {
                    "url":"https://web.immomo.com/webmomo/api/scene/profile/roominfos",
                    "formdata":{
                        "stid":stid,
                        "src":"url",
                    }
                }
    def getRequest(self):
        return self.createRequest()
    def onRequest(self, request):
        if not self.cookie:
            self.cookie = "cId=%d" % time.time()
    def onResponse(self, request, response):
        if response['ec'] in (200,400):
            response['_id'] = request['formdata']['stid']
            super(SceneProfileRoominfos,self).onResponse(request, response)
            #print '{}, {} {}'.format(time.strftime('%H:%M:%S'),
            #                         response['data']['stid'],
            #                         response['data']['name'].encode('utf-8'))
        else:
            raise ResponseError(response['ec'], response['em'])
    def onError(self, request, response):
        print '{}, {}, {}, {}'.format(time.strftime('%H:%M:%S'),
                                    self.proxy,
                                    response['ec'],
                                    response['em'].encode('utf-8'))
            
        time.sleep(10)
        self.cookie = ""
        return request
            
class SceneProfileRoominfosThread(WorkProcess):
    def __init__(self,proxies,lock,status):
        self.lock = lock 
        proxies = multiprocessing.Manager().list(proxies)
        super(SceneProfileRoominfosThread,self).__init__(proxies,lock,status)
    def initRequest(self, proxy, userpwd, db):
        return SceneProfileRoominfos(proxy, userpwd, db, self.lock)

if __name__ == "__main__":
    SceneProfileRoominfos.load("totalstars_clean_" + sys.argv[1])

    status = multiprocessing.Manager().dict()
    lock = multiprocessing.Lock()
    tMonitor = ThreadMonitor(lock,status,SceneProfileRoominfos.TASKS)
    tMonitor.start()

    threads = []
    for ip in range(71,79):
        if ip == 71:
            proxies = map(lambda x:("http://192.168.0.71:3128","u{}:crawl".format(x)), range(28))
        else:
            proxies = map(lambda x:("http://192.168.0.{}:3128".format(ip),"u{}:crawl".format(x)), range(30))
        #proxies = [("http://127.0.0.1:8888","")]
        
        t = SceneProfileRoominfosThread(proxies,lock,status)
        t.start()
        threads.append(t)
    #t = SceneProfileRoominfosThread(HttpsProxy.getProxy())
    #t.start()
    #threads.append(t)
    map(lambda t:t.join(), threads)

