# -*- coding: utf-8 -*-
import re
import os
import sys
import json
import time
import pymongo
from pymongo import CursorType
#from pytz import timezone
from datetime import datetime,timedelta,date
from bisect import bisect_left,bisect_right
import pandas as pd
import numpy as np
from multiprocessing import Process, JoinableQueue, cpu_count, Manager
from threading import Event, Thread
import struct
#import matplotlib.pyplot as plt

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return json.JSONEncoder.default(self, obj)

class ComplexDecoder(json.JSONDecoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return json.JSONDecoder.default(self, obj)

#client = pymongo.MongoClient('mongodb://192.168.0.13')
#db = client['immomo']
quarter = "2020Q1"
Q_ALL = quarter + "_ALL"
Q_FILE = quarter + "_FILE"
Q_ERR = quarter + "_ERR"
Q_totalgap = quarter + "_totalgap"
Q_TEST = datetime.now().strftime('Testing_%Y%m%d%H%M%S')

def load():
    client = pymongo.MongoClient('mongodb://192.168.0.13')
    db = client['immomo']

    db["cleanErr"].drop()
    
    charm = {}
    map(lambda x:charm.setdefault(x["_id"],x),db["gap"].find())
    
    errIds = set(db["product_buy"].distinct("_id.starid",{"response.em":{"$in":["测试号不能和正式号互送","只可以送礼物给主播"]}}))

    query = {
        #"data.momoid": {
        #    "$in": [
        #        "5081208",      #徐泽
        #        "334299598",    #狮大大
        #        "337153000",    #洪小乔
        #        "336400734",    #三八哥
        #        "123745291",    #帝王军
        #        "86977431",     #狼王
        #        "106757414",    #叶哥
        #        "539778699",    #烟花易冷
        #        "30837417",     #活力
        #        "365739735",    #呆梨
        #        "369494527"     #追日
        #    ]
        #},
        "data.charm": {
            "$gte": 6
        }
    }

    names = sorted(filter(lambda name:re.match("^ranking_card_(2019123|2020)\d+$",name), db.collection_names()))
    #names = ["ranking_card_20200413"]
    
    results = {}
    for name in names:
        db[name].create_index([('data.momoid', pymongo.ASCENDING)])
        items = db[name].find(query,
                              {
                                  "_id":0,
                                  "timesec":1,
                                  "data.room_goto":1,
                                  "data.momoid":1,
                                  "data.nick":1,
                                  "data.charm":1,
                                  "data.gap_charm.nextgap":1
                               })
        for i,item in enumerate(items):
            if i % 100000 == 0:
                print ('{} load {} {}w'.format(time.strftime('%H:%M:%S'), name, i / 10000))

            if item["data"]["momoid"] in errIds:
                continue

            try:
                momoid = item["data"]["momoid"]
                level = item["data"]["charm"]
                nextgap = item["data"]["gap_charm"]["nextgap"]
            except:
                item["collection"] = name
                db["cleanErr"].save(item)
                continue

            if not isinstance(nextgap,unicode):
                nextgap = unicode(nextgap)
            m = re.match(u'(\d+)万',nextgap)
            if m:
                nextgap = int(m.group(1)) * 10000

            nextgap = int(nextgap)
            if nextgap == 0:
                nextgap = charm[level]['nextgap']

            roomid = long(0)
            m = re.search('\|(\d+)\|',item["data"]["room_goto"])
            if m:
                roomid = long(m.group(1))

            if momoid not in results:
                results[momoid] = []

            results[momoid].append((roomid,
                                    item["timesec"],
                                    item["data"]["nick"],
                                    level,
                                    charm[level]['totalgap'] + charm[level]['nextgap'] - nextgap))
        #break

    db[Q_ALL].drop()
    for i,(momoid,data) in enumerate(results.iteritems()):
        if i % 10000 == 0:
            print ('{} save {} {}w'.format(time.strftime('%H:%M:%S'), Q_ALL, i / 10000))
        
        result = {"_id":momoid, "data":[]}
        for roomid,timesec,nick,charm,totalgap in data:
            result["data"].append({
                "roomid": roomid,
                "nick": nick,
                "charm": charm,
                "timesec": datetime.utcfromtimestamp(timesec), # mongo存储utc时间
                "totalgap": totalgap
            })
        result["data"].sort(key=lambda x:x["timesec"])
        result["nick"] = result["data"][-1]["nick"]
        db[Q_ALL].save(result)

    print ('{} save {} ok, {}'.format(time.strftime('%H:%M:%S'), Q_ALL, i))

def dispose(tasks,charm,results):
    while True:
        text = tasks.get()
        item = json.loads(text, cls=ComplexDecoder)
        try:
            level = map(lambda x:x["charm"],item["data"])
            totalgap = map(lambda x:x["totalgap"],item["data"])
            # mongo存储utc时间
            index = pd.DatetimeIndex(map(lambda x:x["timesec"],item["data"])).tz_localize('UTC').tz_convert('Asia/Shanghai')
            ts = pd.DataFrame({'totalgap': totalgap, 'charm': level},index)
            ts = ts[ts['totalgap'] > 0] # 剔除0
            ts = ts.groupby(lambda x:x.date).first() # 一天内取最早一条
            totalgap = ts['totalgap']
            anomaly = totalgap[totalgap.diff() < 0]
            cur = 0
            diff = pd.Series()
            for key,value in anomaly.iteritems():
                loc = ts.index.get_loc(key) - 1
                level = ts.iloc[loc]["charm"]

                # 降级规则
                # a) 主播降级前等级为15~17，直接倒退到15级初始状态
                # b) 主播降级前等级>=18，且降级前等级已产生的星光值<下降目标等级的星光值的90%，则倒退3级，保留该级已产生星光值。
                # c)
                # 主播降级前等级>=18，且降级前等级已产生的星光值>=下降目标等级的星光值的90%，则倒退3级，并保留倒退后该等级90%的星光值。
                if level < 15:
                    raise Exception()
                #if ts.index[loc].month <> ts.index[loc + 1].month:
                nextgap = ts.iloc[loc]["totalgap"] - charm[level]["totalgap"]
                if level >= 15 and level <= 17:
                    level = 15
                    nextgap = 0
                elif level >= 18:
                    level -= 3
                    target = charm[level]["nextgap"] * 0.9
                    if nextgap >= target:
                        nextgap = target
                    
                target = charm[level]["totalgap"] + nextgap
                # 星光值精度单位为万，降级之后精度会提高
                if target - value > 10000:
                    # 忽略2019.7.1日异常，使用之后数据修补
                    if key not in (date(2019,7,1),date(2019,10,1)):
                        raise Exception(key)
                    elif loc + 2 < totalgap.count():
                        totalgap[key] = totalgap[loc + 2]
                    else:
                        totalgap[key] = target
                # 切分
                # 线性填充空白日期
                part = totalgap[cur:]
                part = part[part.index < key]
                part = part.reindex(pd.date_range(part.index[0],part.index[-1])).interpolate() 
                cur = totalgap.index.get_loc(key)
                diff = diff.append(part.diff())
        
            part = totalgap[cur:]
            part = part.reindex(pd.date_range(part.index[0],part.index[-1])).interpolate() 
            diff = diff.append(part.diff())
       
            # 填充空值
            diff = diff.fillna(0)

            # 过滤
            if diff.sum() == 0:
                continue

            data = {"_id":item["_id"]}
            for index,totalgap in diff[1:].iteritems():
                data[(index - timedelta(1)).strftime("%Y-%m-%d")] = totalgap
        
            results.put(json.dumps(data))
        except Exception,ex:
            print item["_id"],ex
            continue
        finally:
            tasks.task_done()

    print ('{} dispose ok'.format(time.strftime('%H:%M:%S')))

def save(results, exit):
    client = pymongo.MongoClient('mongodb://192.168.0.13')
    db = client['immomo']
    data = []
    count = 0
    while True:
        try:
            text = results.get(timeout=1.0)
            data.append(json.loads(text))
            if len(data) >= 1000:
                raise None
        except:
            if data:
                db[Q_totalgap].insert(data)
                count += len(data)
                data = []
                print ('{} save {} {}'.format(time.strftime('%H:%M:%S'), Q_totalgap, count))
            else:
                try:
                    exit.wait(timeout=0)
                    print ('{} save {} {} complete'.format(time.strftime('%H:%M:%S'), Q_totalgap, count))
                    break
                except:
                    continue

def totalgap():
    client = pymongo.MongoClient('mongodb://192.168.0.13')
    db = client['immomo']

    db[Q_ERR].drop()
    db[Q_totalgap].drop()

    manager = Manager()
    charm = manager.dict()
    map(lambda x:charm.setdefault(x["_id"],x),db["gap"].find())

    errIds = set(db["product_buy"].distinct("_id.starid",{"response.em":{"$in":["测试号不能和正式号互送","只可以送礼物给主播"]}}))

    tasks = JoinableQueue()
    results = JoinableQueue()
    exit = Event()

    for i in range(cpu_count()):
        p = Process(target=dispose,args=(tasks,charm,results))
        p.daemon = True
        p.start()

    t = Thread(target=save, args=(results,exit))
    t.start()

    items = db[Q_ALL].find()
    for item in items:
        if item["_id"] in errIds:
            continue
        tasks.put(json.dumps(item,cls = ComplexEncoder))
    
    tasks.join()
    exit.set()
    t.join()

def sum():
    client = pymongo.MongoClient('mongodb://192.168.0.13')
    db = client['immomo']

    items = db[Q_totalgap].find({},{"_id":0})
    df = pd.DataFrame(items)
    #df = df.diff()
    df = df.fillna(0)
    df = df.sum().groupby(lambda x:pd.to_datetime(x)).first()
    df = df.groupby(lambda x:x.date).first()
    df = df.rename_axis("date").rename("totalgap")
    with pd.ExcelWriter(datetime.now().strftime('immomo_quarterly_%Y%m%d_{}.xlsx'.format(quarter))) as writer:
        df.to_excel(writer)

def error():
    client = pymongo.MongoClient('mongodb://192.168.0.13')
    db = client['immomo']

    charm = {}
    map(lambda x:charm.setdefault(x["_id"],x),db["gap"].find())

    items = db[Q_ALL].find({"_id" : "420834495"})
    for item in items:
        try:
            level = map(lambda x:x["charm"],item["data"])
            totalgap = map(lambda x:x["totalgap"],item["data"])
            #index = pd.DatetimeIndex(map(lambda x:x["timesec"],item["data"]))
            index = pd.DatetimeIndex(map(lambda x:x["timesec"],item["data"])).tz_localize('UTC').tz_convert('Asia/Shanghai')
            ts = pd.DataFrame({'totalgap': totalgap, 'charm': level},index)
            ts = ts[ts['totalgap'] > 0] # 剔除0
            ts = ts.groupby(lambda x:x.date).first() # 一天内取最早一条
            totalgap = ts['totalgap']
            anomaly = totalgap[totalgap.diff() < 0]
            cur = 0
            diff = pd.Series()
            for key,value in anomaly.iteritems():
                loc = ts.index.get_loc(key) - 1
                level = ts.iloc[loc]["charm"]

                # 降级规则
                # a) 主播降级前等级为15~17，直接倒退到15级初始状态
                # b) 主播降级前等级>=18，且降级前等级已产生的星光值<下降目标等级的星光值的90%，则倒退3级，保留该级已产生星光值。
                # c)
                # 主播降级前等级>=18，且降级前等级已产生的星光值>=下降目标等级的星光值的90%，则倒退3级，并保留倒退后该等级90%的星光值。
                if level < 15:
                    raise Exception()
                #if ts.index[loc].month <> ts.index[loc + 1].month:
                nextgap = ts.iloc[loc]["totalgap"] - charm[level]["totalgap"]
                if level >= 15 and level <= 17:
                    level = 15
                    nextgap = 0
                elif level >= 18:
                    level -= 3
                    target = charm[level]["nextgap"] * 0.9
                    if nextgap >= target:
                        nextgap = target
                    
                target = charm[level]["totalgap"] + nextgap
                # 星光值精度单位为万，降级之后精度会提高
                if target - value > 10000:
                    # 忽略2019.7.1日异常，使用之后数据修补
                    if key not in (date(2019,7,1),date(2019,10,1)):
                        raise Exception(key)
                    elif loc + 2 < totalgap.count():
                        totalgap[key] = totalgap[loc + 2]
                    else:
                        totalgap[key] = target
                # 切分
                # 线性填充空白日期
                part = totalgap[cur:]
                part = part[part.index < key]
                part = part.reindex(pd.date_range(part.index[0],part.index[-1])).interpolate() 
                cur = totalgap.index.get_loc(key)
                diff = diff.append(part.diff())
        
            part = totalgap[cur:]
            part = part.reindex(pd.date_range(part.index[0],part.index[-1])).interpolate() 
            diff = diff.append(part.diff())
       
            # 填充空值
            diff = diff.fillna(0)

            # 过滤
            if diff.sum() == 0:
                continue

            data = {"_id":item["_id"]}
            for index,totalgap in diff[1:].iteritems():
                data[(index - timedelta(1)).strftime("%Y-%m-%d")] = totalgap
        
            results.put(json.dumps(data))
        except Exception,ex:
            print item["_id"],ex
            continue
    #fig,ax = plt.subplots(2,1)
    #ax[0].plot(ts["totalgap"])
    #ax[1].plot(ts["charm"])
    #plt.show()
    pass
if __name__ == '__main__':
    load()
    totalgap()
    sum()
    #error()
    #group()
