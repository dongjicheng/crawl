#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import pycurl

import re
import sys

from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.tools import process_manger
from multiprocess.tools import timeUtil, collections
import random
import time
from fake_useragent import UserAgent
import logging
from multiprocess.core import HttpProxy
strings="""<li id="brand-13630" data-initial="O" style="display:block;">
									<a href="/list.html?cat=14065%2C14086%2C14098&ev=exbrand_%E6%AC%A7%E5%BE%B7%E7%B4%A0%EF%BC%88AUTOTRIO%EF%BC%89%5E&cid3=14098"
 rel="nofollow" onclick="searchlog(1,0,13630,71,'品牌::欧德素（AUTOTRIO）')" title="欧德素（AUTOTRIO）">
										<i></i>
										欧德素（AUTOTRIO）
									</a>
								</li>
								<li id="brand-112832" data-initial="S" style="display:block;">
									<a href="/list.html?cat=14065%2C14086%2C14098&ev=exbrand_%E4%B8%89%E5%92%8C%EF%BC%88SANO%EF%BC%89%5E&cid3=14098"
 rel="nofollow" onclick="searchlog(1,0,112832,71,'品牌::三和（SANO）')" title="三和（SANO）">
										<i></i>
										三和（SANO）
									</a>
								</li>"""
a=re.findall(r'<li id="brand-(\d+)[\s\S]*?品牌::([\s\S]*?)\'\)"',strings, flags=re.MULTILINE)
allcnt_pattern = re.compile(r'"commentCount":(\d+),')
sss="""fetchJSON_comment98({"productAttr":[{"unit":"厘米","name":"身高","type":"people_height","key":"firstAttribute"},{"unit":"公斤","name":"体重","type":"people_weight","key":"secondAttribute"}],"productCommentSummary":{"skuId":68560776116,"averageScore":5,"defaultGoodCount":1409,"defaultGoodCountStr":"1400+","commentCount":1554,"commentCountStr":"1500+","goodCount":141,"goodCountStr":"100+","goodRate":0.97,"goodRateShow":97,"generalCount":2,"generalCountStr":"2","generalRate":0.017,"generalRateShow":2,"poorCount":2,"poorCountStr":"2","poorRate":0.013,"poorRateShow":1,"videoCount":1,"videoCountStr":"1","afterCount":2,"afterCountStr":"2","showCount":12,"showCountStr":"10+","oneYear":0,"sensitiveBook":0,"fixCount":0,"plusCount":0,"plusCountStr":"0","buyerShow":0,"poorRateStyle":2,"generalRateStyle":3,"goodRateStyle":145,"installRate":0,"productId":68560776116,"score1Count":2,"score2Count":0,"score3Count":2,"score4Count":4,"score5Count":137},"hotCommentTagStatistics":[{"id":"8ac55c1a040a27fb","name":"清薄透气","count":3,"type":4,"canBeFiltered":true,"stand":1,"rid":"8ac55c1a040a27fb","ckeKeyWordBury":"eid=100^^tagid=8ac55c1a040a27fb^^pid=20003^^sku=68560776116^^sversion=1000^^token=0337433b3f5e5c45"},{"id":"2a7da6f722642413","name":"穿戴舒服","count":3,"type":4,"canBeFiltered":true,"stand":1,"rid":"2a7da6f722642413","ckeKeyWordBury":"eid=100^^tagid=2a7da6f722642413^^pid=20003^^sku=68560776116^^sversion=1000^^token=22066ce5e09cf08f"},{"id":"4d59f8f1b31b5aeb","name":"美丽大方","count":2,"type":4,"canBeFiltered":true,"stand":1,"rid":"4d59f8f1b31b5aeb","ckeKeyWordBury":"eid=100^^tagid=4d59f8f1b31b5aeb^^pid=20003^^sku=68560776116^^sversion=1000^^token=15e3c7c10d05f033"},{"id":"4451ec958e2b2933","name":"可爱甜美","count":1,"type":4,"canBeFiltered":true,"stand":1,"rid":"4451ec958e2b2933","ckeKeyWordBury":"eid=100^^tagid=4451ec958e2b2933^^pid=20003^^sku=68560776116^^sversion=1000^^token=39b85e25d73b8e27"},{"id":"21440c96df643004","name":"柔软舒"""
print(allcnt_pattern.findall(sss))


class GetComment2(SpiderManger):
    def __init__(self, seeds_file, dateindex, **kwargs):
        super(GetComment2, self).__init__(**kwargs)
        self.ua = UserAgent()
        with open(seeds_file) as infile:
            data_set = collections.DataSet(infile)
            for i, seed in enumerate(data_set.map(lambda line: line.strip('\n').split("\t")[0])
                                             .shuffle(2048)):
                self.seeds_queue.put(Seed(seed, kwargs["retries"]))
        self.allcnt_pattern = re.compile(r'"commentCount":(\d+),')
        self.dateindex = dateindex

    def make_request(self, seed):
        url = "https://club.jd.com/comment/skuProductPageComments.action?callback=fetchJSON_comment98&productId={0}&score=0&sortType=5&page=0&pageSize=10&isShadowSku=0&fold=1".format(seed.value)
        request={"url": url,
         "encoding":"utf-8",
         "method":"get",
         "proxy": self.used_proxy,
         "headers":{
         'Host': 'club.jd.com',
         'Connection': 'close',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
         'Referer': 'https://item.jd.com/{0}.html'.format(seed.value)
        }}
        return request

    def parse_item(self, content, seed):
        result = []
        count = self.allcnt_pattern.findall(content)
        if not count:
            result.append({"skuid": seed.value, "comment": "0"})
        else:
            result.append({"skuid": seed.value, "comment": str(count[0])})
        #r = (0.1563 + random.random() / 10)
        #time.sleep(r)
        if result:
            return result
        else:
            return [{"seed": seed.value}]

current_date = timeUtil.current_time()
new_config = {"job_name": "jdcomment"
    , "spider_num": 0
    , "use_new_download_api": True
    , "retries": 3
    , "pycurl_config": {pycurl.CONNECTTIMEOUT: 2, pycurl.TIMEOUT: 10}
    , "complete_timeout": 5 * 60
    , "sleep_interval": 0
    , "rest_time": 5
    , "write_seed": False
    , "seeds_file": "resource/month202006"
    , "dateindex": current_date
    , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jicheng",
                       "collection": "comment" + current_date}
    , "log_config": {"level": logging.ERROR, "filename": sys.argv[0] + '.logging', "filemode": 'a',
                     "format": '%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
    , "proxies_pool": HttpProxy.getHttpProxy()
    , "use_proxy": True
              }
p = GetComment2(**new_config)
request = {"url": "https://list.jd.com/list.html?cat=1316%2C1381%2C1392&ev=exbrand_%E8%86%9C%E6%B3%95%E4%B8%96%E5%AE%B6%EF%BC%88Mask%20Family%201908%EF%BC%89%5E&page=3&s=31&click=1",
                   "proxy": "http://u0:crawl@192.168.0.71:3128",
                   "headers":{
         'Connection': 'close',
         'User-Agent': p.ua.chrome,
'Referer': 'https://list.jd.com/list.html?cat=1316%2C1381%2C1392&ev=exbrand_%E8%86%9C%E6%B3%95%E4%B8%96%E5%AE%B6%EF%BC%88Mask%20Family%201908%EF%BC%89%5E&page=3&s=60&click=1'
        }}
request = {"url": "https://list.jd.com/listNew.php?cat=1316%2C1381%2C1392&ev=exbrand_%E5%B0%8F%E8%BF%B7%E7%B3%8A%5E&page=2&s=31&scrolling=y&log_id=1596108547754.6591&tpl=1_M&isList=1&show_items=1862177,4218073,3471629,12036806817,100003909365,15347426728,30506593541,65713312691,1127689,10600075508,12035707146,100002190765,100012283842,4796527,30505508019,2198113,100005786826,66740850270,100006686863,66823223420,100005423924,100006803885,30511502718,15393976502,68399318137,100009222686,66814315082,100009222654,100009909334,100006690755",
                   "proxies": {"http":"http://u0:crawl@192.168.0.71:3128"},
           "method":"get",
                   "headers":{
         'Connection': 'close',
         'User-Agent': p.ua.chrome,
'Referer': 'https://list.jd.com/list.html?cat=1316%2C1381%2C1392&ev=exbrand_%E5%B0%8F%E8%BF%B7%E7%B3%8A%5E&page=1&s=1&click=1'
        }}
import urllib
print(urllib.parse.urlencode({"ev":"exbrand_膜法世家（Mask Family 1908）^"}))
print(urllib.parse.urlencode({"cat":"1316,1381,1392"}))

first_pettern = re.compile(r"search000014_log:{wids:'([,\d]*?)',")
#r = p.download(request)
r = p.do_request(request)
print(first_pettern.findall(r))
totalpage_perttern = re.compile(r'<div id="J_topPage"[\s\S]*?<b>\d+</b><em>/</em><i>(\d+)</i>11')
print(totalpage_perttern.findall(r))
