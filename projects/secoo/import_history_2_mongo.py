#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mongo import op
import os
import glob
file_names = glob.glob("List20??-??-??")
with op.DBManger() as db:
    for file in glob.glob("List20??-??-??"):
        print("importing file: " + file)
        for line in open(file):
            line = line.strip("\n").split("\t")
            db.insert_one(("secoo",file),data_tupe=tuple(line),fields=("pid","name","lo","self","price"))
