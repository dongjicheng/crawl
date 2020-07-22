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

with op.DBManger() as m:
    for i in m.read_from(("jicheng","jdprice"),pipeline=pipeline,out_field=("_id","count")):
        print(i)