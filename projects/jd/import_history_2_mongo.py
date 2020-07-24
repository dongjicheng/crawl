#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
import os
import glob
os.chdir("/home/u9000/martingale/jd_month/")
with op.DBManger() as db:
    db.load_file_to_db(filename="month202006", db_collect=("jingdong", "totalskuids"),sep="\t",buffer_size=128,
                       column_index_list=[0], fields_tupe=("skuid","name","lo","self","price"),attach_dict={"_date":date})