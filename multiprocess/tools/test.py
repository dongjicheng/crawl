#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocess.tools import timeUtil,collections
print(timeUtil.getdate(0,format="%Y-%m%d"))
print(timeUtil.current_time())
dt = collections.DataSet(range(100))
for i in dt.shuffle(buffer_size=3).map(lambda x: x*2).map(lambda x: x+1):
    print(i)
