#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
import os
import glob
os.chdir("/home/u9000/secoo/secoo/list_table/")
file_names = glob.glob("List20??-??-??")
with op.DBManger() as db:
    for file in glob.glob("List20??-??-??"):
        newfile = file.replace("-","")
        print("importing file: " + file)
        date = newfile[4:]
        db.load_file_to_db(filename=file, db_collect=("secoo", newfile),sep="\t",buffer_size=128,
                           fields_tupe=("pid","name","lo","self","price"),attach_dict={"_date":date})