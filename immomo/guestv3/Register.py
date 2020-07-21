# -*- coding: utf-8 -*-
import json
import time
import datetime
import pymongo
import sys

client = pymongo.MongoClient('mongodb://192.168.0.13')
db = client['immomo']
cout = "register_" + sys.argv[1]
db[cout].drop()

items = db['totalstars_clean_' + sys.argv[1]].find()#{"_id" : "392902646"}
results = []
for i,item in enumerate(items):
    if i % 100000 == 0:
        print '{} load {} {}w'.format(time.strftime('%H:%M:%S'), "totalstars_clean_20180123", i/10000)

    register = 0.0
    if 'roomid' in item and item['roomid']:
        register = float(item['roomid'][0:10] + '.' + item['roomid'][10:])
    item["register"] = datetime.datetime.fromtimestamp(register)
    results.append(item)

    if len(results) > 1000:
        db[cout].insert(results)
        results = []
if results:
    db[cout].insert(results)
    