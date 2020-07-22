#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymongo
from multiprocess.config import  default_config


class DBManger(object):

    def __init__(self, mongo_addr=default_config.config["mongo_config"]["addr"]
                 , db_collection=("jicheng","tmp")):
        self.client = pymongo.MongoClient(mongo_addr)
        self.collection = self.client[db_collection[0]][db_collection[1]]

    def get_client(self):
        return self.client

    def switch_db_collection(self, db_collection):
        self.collection = self.client[db_collection[0]][db_collection[1]]

    def close(self):
        self.client.close()

    def read_from(self, db_collect, out_field, pipeline=None):
        self.switch_db_collection(db_collect)
        if pipeline is None:
            result = map(lambda x: tuple([x[field] for field in out_field]),
                        self.collection.find({}))
        else:
            result = map(lambda x: tuple([x[field] for field in out_field]),
                        self.collection.aggregate(pipeline, allowDiskUse=True))
        return result

    def insert_one(self, db_collect, data_dict):
        self.switch_db_collection(db_collect)
        self.collection.insert_one(data_dict)

    def insert_many(self, db_collect, data_dict_list):
        self.switch_db_collection(db_collect)
        self.collection.insert_many(data_dict_list)

    def insert_one(self, db_collect, data_tupe, fields):
        self.switch_db_collection(db_collect)
        self.collection.insert_one(dict(zip(fields, data_tupe)))

    def insert_many(self, db_collect, data_tupe_list, fields):
        self.switch_db_collection(db_collect)
        self.collection.insert_many([dict(zip(fields, data_tupe))for data_tupe in data_tupe_list])

    def __enter__(self):
        return self

    def __exit__(self, Type, value, traceback):
        self.client.close()
