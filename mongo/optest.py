#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
pipeline = [
    {
        "$match": {
            "_status": 0
        }
    },
    {"$group": {"_id": "$skuIds"}},
    {"$group": {"_id": 1, "count": {"$sum": 1}}},
    {"$limit": 5000000}
]

with op.DBManger() as m, op.DBManger() as n:
    for i in m.read_from(db_collect=("jicheng","jdprice"), out_field=("count",), pipeline=pipeline):
        #n.insert_one(db_collect=("jicheng","jdprice1"), data_tupe=i, fields=("count",))
        #n.insert_many(db_collect=("jicheng", "jdprice1"), data_tupe_list=[i], fields=("count",))
        pass