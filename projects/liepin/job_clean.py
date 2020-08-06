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
    print(m.get_lasted_collection("lieping", filter={"name": {"$regex": r"^comment20200728$"}}))