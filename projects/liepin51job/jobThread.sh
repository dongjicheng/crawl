#!/usr/bin/env bash
cd `dirname $0`
nohup python2 jobThread.py >> jobThread.py.log 2>&1 &
