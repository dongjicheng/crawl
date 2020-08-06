#!/usr/bin/env bash
cd `dirname $0`
source /home/u9000/anaconda3/bin/activate jicheng2
nohup python company.py >> company.py.log 2>&1 &
nohup python jobThread.py >> jobThread.py.log 2>&1 &
