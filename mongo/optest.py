#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
from tqdm import tqdm
pipeline = [
    {
        "$match": {
            "code": 0
        }
    },
    {"$group": {"_id": "$skuIds"}},
    {"$group": {"_id": 1, "count": {"$sum": 1}}},
    {"$limit": 5000000}
]

with op.DBManger() as m:
    for p in m.read_from(db_collect=("secoo","CleanListNew"),out_field=("pid",)):
        print(p)

def aaa():
    pass

print(aaa())