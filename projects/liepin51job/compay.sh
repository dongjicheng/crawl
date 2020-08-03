#!/usr/bin/env bash
cd `dirname $0`
nohup python2 company.py >> company.py.log 2>&1 &
