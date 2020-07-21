# -*- coding: utf-8 -*-
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import json
import time
import pymongo
import Queue
import threading

class MomoServer(BaseHTTPRequestHandler):
    queue = Queue.PriorityQueue()
    db = pymongo.MongoClient('mongodb://192.168.0.13')['immomo']
    send = 0
    recv = 0
    lock = threading.Lock()
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        if self.path.startswith("/immomo"):
            totalgap,starid,roomid = MomoServer.queue.get()
            self.wfile.write(json.dumps([{
                "starid": starid, 
                "roomid": roomid
            }]))
            with MomoServer.lock:
                MomoServer.send += 1
                print "Send: {} Recv: {}, Queue: {}".format(MomoServer.send, MomoServer.recv, MomoServer.queue.qsize())
        threading._sleep(0.5)
        pass
    def do_POST(self):
        if self.path.startswith("/v3/room/product/buy"):
            data = json.loads(self.rfile.read(int(self.headers["content-length"])))
            data["_id"] = {
                "starid":data["request"]["starid"],
                "roomid":data["request"]["roomid"]
            }
            
            MomoServer.db["product_buy"].save(data)
            MomoServer.queue.task_done()

            print "Recv:",data["request"]["starid"],data["response"]["em"]
            with MomoServer.lock:
                MomoServer.recv += 1
        elif self.path.startswith("/NoSuchMethodError"):
            data = json.loads(self.rfile.read(int(self.headers["content-length"])))
            MomoServer.db["NoSuchMethodError"].save(data)

            #MomoServer.queue.put((data["totalgap"],data["starid"],data["roomid"]))
            #MomoServer.queue.task_done()
            #print "NoSuchMethodError:",json.dumps(data)
        self.send_response(200)
        self.end_headers()
        pass

class ThreadingServer(ThreadingMixIn, HTTPServer):
    pass

if __name__ == '__main__':
    #cursor_type=pymongo.CursorType.EXHAUST
    pipeline = [
        {
            "$project": {
                "_id": {
                    "starid": "$_id.momoid", 
                    "roomid": "$_id.roomid"
                }, 
                "charm": 1
            }
        }, 
        {
            "$lookup": {
                "from": "product_buy", 
                "localField": "_id", 
                "foreignField": "_id", 
                "as": "product"
            }
        }, 
        {
            "$match": {
                "_id.roomid": {
                    "$ne": None
                }, 
                "product": [ ]
            }
        }, 
        #{
        #    "$limit": 10
        #}
    ]
    items = MomoServer.db["roomids"].aggregate(pipeline,allowDiskUse=True)
    for i,item in enumerate(items):
        if i % 10000 == 0:
            print ('{} load {} {}w'.format(time.strftime('%H:%M:%S'), "roomids", i/10000))
        MomoServer.queue.put((-item["charm"],item["_id"]["starid"],item["_id"]["roomid"]))

    print ('{} load {} complete, {}'.format(time.strftime('%H:%M:%S'), "roomids", MomoServer.queue.qsize()))
    server = ThreadingServer(('0.0.0.0', 9999), MomoServer)
    server.serve_forever()
    MomoServer.queue.join()
