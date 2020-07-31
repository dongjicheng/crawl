#!/usr/bin/env bash
cd `dirname $0`
source /home/u9000/anaconda3/bin/activate jicheng
nohup python jd_wap.py >> jd_wap.py.log 2>&1 &
