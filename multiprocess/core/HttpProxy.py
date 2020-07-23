#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import random

def getProxy():
    proxies = []
    proxies.extend(map(lambda x:("http://u{0}:crawl@192.168.0.71:3128".format(x)), range(28)))
    #proxies.extend(map(lambda x:("http://u{1}:crawl@192.168.0.{0}:3128".format(x[0],x[1])),itertools.product(range(72,79),range(30))))
    random.shuffle(proxies)
    return proxies