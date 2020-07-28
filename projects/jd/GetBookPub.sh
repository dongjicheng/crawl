#!/usr/bin/env bash
cd `dirname $0`
source activate jicheng
nohup python GetBookPub.py >> GetBookPub.py.log 2>&1 &
