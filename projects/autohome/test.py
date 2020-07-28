#!/usr/bin/env python
# -*- coding: utf-8 -*-
proxy=[]
def get_proxy():
    #global proxy
    proxy.extend(map(lambda x:("http://crawl:u{}@192.168.0.{}:3128").format(x,72),range(28)))
get_proxy()
print(proxy)