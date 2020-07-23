#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime

def getdate(beforedays, currentday=None, format="%Y%m%d"):
    # 获取前beforeOfDay天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天
    if currentday:
        today = datetime.datetime.strptime(currentday, format)
    else:
        today = datetime.datetime.now()

    re_date = (today + datetime.timedelta(days=beforedays)).strftime(format)
    return re_date

def current_time(format="%Y%m%d"):
    return getdate(0, format=format)
