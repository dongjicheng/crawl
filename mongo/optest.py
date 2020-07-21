#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo


myclient = pymongo.MongoClient("mongodb://192.168.0.13:27017")
shoujiguishudi = myclient["jicheng"]["shoujiguishudi"]
dblist = myclient.list_database_names()
for line in open("../a.1"):
    items = line.strip().split("\t")
    #id=shoujiguishudi.insert_many([{"phonenumber":items[0], "province":items[1], "city":items[2] , "company":items[3]}])
    id = shoujiguishudi.insert_many(
        [{}])

print(bool([{}]))