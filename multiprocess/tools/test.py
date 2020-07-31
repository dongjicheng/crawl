#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocess.tools import timeUtil,collections
from mongo import op
print(timeUtil.getdate(0,format="%Y-%m%d"))
print(timeUtil.current_time())
dt = collections.DataSet([1,2,3,4,5,6,7,8,9,1,2,3,4,5,6])
for i in dt.shuffle(buffer_size=3).map(lambda x: x*2).map(lambda x: x+1).distinct():
    print(i)

with op.DBManger() as m:
    last_brand_collect = m.get_lasted_collection("jingdong", filter={"name": {"$regex": r"^brand20\d\d\d\d\d\d$"}})
    pipeline = [
        {"$match":{"cate_id": {"$ne": None}}},
        {"$match":{"brand_id": {"$ne": None}}},
        {"$match":{"name": {"$ne": None}}},
        {"$match":{"_status": 0}}
    ]
    print("jingdong", last_brand_collect)
    data_set = collections.DataSet(m.read_from(db_collect=("jingdong", last_brand_collect),
                            out_field=("cate_id",'brand_id',"name"), pipeline=pipeline))
    for i,seed in enumerate(data_set.distinct()):
        print(i,seed)
