#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymongo
from multiprocess.config import default_config


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

    def insert_one_dict(self, db_collect, data_dict):
        self.switch_db_collection(db_collect)
        self.collection.insert_one(data_dict)

    def insert_many_dict(self, db_collect, data_dict_list):
        self.switch_db_collection(db_collect)
        self.collection.insert_many(data_dict_list)

    def insert_one_tupe(self, db_collect, data_tupe, fields):
        self.switch_db_collection(db_collect)
        self.collection.insert_one(dict(zip(fields, data_tupe)))

    def insert_many_tupe(self, db_collect, data_tupe_list, fields):
        self.switch_db_collection(db_collect)
        self.collection.insert_many([dict(zip(fields, data_tupe))for data_tupe in data_tupe_list])

    def drop_db_collect(self, db_collect):
        self.client[db_collect[0]].drop_collection(db_collect[1])

    def load_file_to_db(self, filename, db_collect, fields_tupe, sep="\t", buffer_size=64, attach_dict=None):
        cache = []
        if attach_dict:
            safe_attach_dict = {}
            for key in attach_dict.keys():
                if key in fields_tupe:
                    safe_attach_dict["_" + key] = attach_dict[key]
                else:
                    safe_attach_dict[key] = attach_dict[key]
        for line in open(filename):
            line = line.strip("\n").split(sep)
            date = dict(zip(fields_tupe, line))
            if safe_attach_dict:
                date.update(safe_attach_dict)
            cache.append(date)
            if len(cache) == buffer_size:
                self.insert_many_dict(db_collect, data_dict_list=cache)
                cache = []
        if cache:
            self.insert_many_dict(db_collect, data_dict_list=cache)

    def __enter__(self):
        return self

    def __exit__(self, Type, value, traceback):
        self.client.close()
