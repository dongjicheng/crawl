#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocess.core import HttpProxy
import logging
config = {"job_name": "shoujiguishudi"
              , "spider_num": 3
              , "retries": 3
              , "request_timeout": 3
              , "completetimeout": 5*60
              , "seeds_file": "resource/buyer_phone.3"
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jicheng"}
              , "proxies": HttpProxy.getProxy()
              , "log_config": {"level": logging.DEBUG, "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              , "headers":{"Connection":"close"}}