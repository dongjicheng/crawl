# -*- coding: utf-8 -*-
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
import traceback
import StringIO
import re
import urllib
import logging
import sys
import datetime

client = pymongo.MongoClient('mongodb://192.168.0.13')
db = client['immomo']

def insert(collection,results):
    try:
        db[collection].insert(results)
    except pymongo.errors.DuplicateKeyError,ex:
        for result in results:
            db[collection].save(result)

def clean(collection):
    pipeline = [
        {
            "$match": {
                "data.charm": {
                    "$gt": 6
                }
            }
        }, 
        {
            "$project": {
                "_id": {
                    "momoid": "$data.momoid", 
                    "timesec": "$timesec"
                }, 
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
                }, 
                "nick": "$data.nick", 
                "charm": "$data.charm", 
                "fansCount": "$data.fansCount", 
                "percent": "$data.gap_charm.percent", 
                "nextgap": "$data.gap_charm.nextgap"
            }
        }
    ]

    items = db[collection].aggregate(pipeline)
    results = []
    for i, item in enumerate(items):
        if i % 10000 == 0:
            print time.strftime('LOAD: %Y-%m-%d %H:%M:%S'), collection, i
        results.append(item)

    for i in range(0,len(results),1000):
        print time.strftime('SAVE: %Y-%m-%d %H:%M:%S'), collection, i
        insert("2018Q4",results[i:i+1000])
        #db["2018Q4"].insert(results[i:i+1000])

if __name__ == '__main__':
    for collection in sorted(filter(lambda name:re.match("^ranking_card_2018\d{4}$",name), db.collection_names())):
        clean(collection)
