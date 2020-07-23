#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
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
    print(m.list_tables("secoo",filter={"name": {"$regex": r"List20\d\d\d\d\d\d"}}))