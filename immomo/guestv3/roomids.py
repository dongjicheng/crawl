# -*- coding: utf-8 -*-
import re
import sys
import json
import time
import pymongo
from pymongo import CursorType
from datetime import datetime,timedelta

client = pymongo.MongoClient('mongodb://192.168.0.13')
db = client['immomo']

pipeline = [
    {
        "$project": {
            "_id": {
                "momoid": "$data.momoid", 
                "roomid": {
                    "$arrayElemAt": [
                        {
                            "$split": [
                                "$data.room_goto", 
                                "|"
                            ]
                        }, 
                        2
                    ]
                }
            }, 
            "charm": "$data.charm", 
            "nick": "$data.nick", 
            "time": {
                "$dateToString": {
                    "format": "%Y-%m-%d", 
                    "date": "$_id", 
                    "timezone": "Asia/Shanghai"
                }
            }
        }
    }, 
    {
        "$match": {
            "_id.roomid": {
                "$exists": True
            }
        }
    }
]

names = sorted(filter(lambda name:re.match("^ranking_card_2018\\d{4}$",name), db.collection_names()))
results = {}
for collection in names:
    items = db[collection].aggregate(pipeline)
    for i,item in enumerate(items):
        if i % 10000 == 0:
            print time.strftime('%Y-%m-%d %H:%M:%S'), collection, i

        id = (item["_id"]["momoid"],item["_id"]["roomid"])
        if id not in results:
            results[id] = item
        elif results[id]["time"] < item["time"]:
            results[id] = item

data = results.values()
for i in range(0,len(data),1000):
    db["roomids"].insert(data[i:i+1000])
