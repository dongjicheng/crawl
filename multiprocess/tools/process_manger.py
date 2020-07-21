#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
def kill_old_process(python_file_name):
    # kill old process
    cur = os.getpid()
    cmd = "ps -ef|grep %s|grep -v grep|awk '{print $2}'" % python_file_name
    for pid in os.popen(cmd):
        if int(pid) != int(cur):
            os.system("kill -9 %s" % pid)
