# -*- coding: utf-8 -*-
import itertools
import random

def getLocal():
    return [("http://127.0.0.1:8888","")]
def getProxy():
    proxies = []
    proxies.extend(map(lambda x:("http://192.168.0.71:3128","u{}:crawl".format(x)), range(28)))
    proxies.extend(map(lambda x:("http://192.168.0.{}:3128".format(x[0]),"u{}:crawl".format(x[1])), 
                     itertools.product(range(72,79),range(30))))
    random.shuffle(proxies)
    return proxies
