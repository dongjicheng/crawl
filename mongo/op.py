#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymongo
from multiprocess.config import  default_config
def get_new_client(mongo_addr=default_config.config["mongo_config"]["addr"]):
    return pymongo.MongoClient(mongo_addr)

def read_from(db_collect, out_field, pipeline=None, client=None):
    if client == None:
        client = get_new_client()
        collction = get_new_client()[db_collect[0]][db_collect[1]]
        if pipeline is None:
            reslt = map(lambda x: tuple([x[field] for field in out_field]),
                        collction.find({}))
        else:
            reslt = map(lambda x: tuple([x[field] for field in out_field]),
                        collction.aggregate(pipeline, allowDiskUse=True))
        client.close()
    else:
        collction = client[db_collect[0]][db_collect[1]]
        if pipeline is None:
            reslt = map(lambda x: tuple([x[field] for field in out_field]),
                        collction.find({}))
        else:
            reslt = map(lambda x: tuple([x[field] for field in out_field]),
                        collction.aggregate(pipeline, allowDiskUse=True))
    return reslt
