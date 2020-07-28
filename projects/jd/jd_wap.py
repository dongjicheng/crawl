#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import sys

from multiprocess.core.spider import SpiderManger, Seed
from multiprocess.tools import process_manger
from multiprocess.tools import timeUtil, collections
import random
import time
from fake_useragent import UserAgent


class GetComment(SpiderManger):
    def __init__(self, seeds_file, dateindex, **kwargs):
        super(GetComment, self).__init__(**kwargs)
        self.proxies = list(map(lambda x:("http://u{}:crawl@192.168.0.71:3128".format(x)), range(28)))
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
         "method":"get",
         "proxies":{"http": random.choice(self.proxies)},
         "headers":{
         'Host': 'club.jd.com',
         'Connection': 'keep-alive',
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


class GetComment1(SpiderManger):
    def __init__(self, seeds_file, dateindex, **kwargs):
        super(GetComment1, self).__init__(**kwargs)
        self.proxies = list(map(lambda x:("http://u{}:crawl@192.168.0.71:3128".format(x)), range(28)))
        self.ua = UserAgent()
        with open(seeds_file) as infile:
            data_set = collections.DataSet(infile)
            for i, seed in enumerate(data_set.map(lambda line: line.strip('\n').split("\t")[0])
                                             .shuffle(2048)):
                self.seeds_queue.put(Seed(seed, kwargs["retries"]))
        self.allcnt_pattern = re.compile(r'"CommentCount": "(\d+)"')
        self.dateindex = dateindex

    def make_request(self, seed):
        url = 'https://wq.jd.com/commodity/comment/getcommentlist?' \
              'callback=skuJDEvalB&pagesize=10&sceneval=2&' \
              'skucomment=1&score=0&sku={}&sorttype=5&page=1'.format(seed.value)
        request={"url": url,
         "proxies":{"http": random.choice(self.proxies)},
         "headers":{
         'Host': 'wq.jd.com',
         'Connection': 'keep-alive',
         'Cookie':'__jd_ref_cls=; mba_muid=15583417484571420227102; mba_sid=15583417484721681913085719337.7; sk_history=4058079%2C; PPRD_P=UUID.15583417484571420227102-LOGID.1558343259629.952955740; __jda=122270672.15583417484571420227102.1558341748.1558341748.1558341748.1; __jdb=122270672.7.15583417484571420227102|1.1558341748; __jdc=122270672; __wga=1558343259612.1558341778580.1558341778580.1558341778580.6.1; shshshfp=7f60d4b70549973377eed5c5b675dbb1; shshshfpb=i9e%2FPQKMi0jSwADEYdV5hQA%3D%3D; shshshsID=9ca7b811c7f566ed6a017b5a67d8d030_6_1558343259935; cid=9; retina=1; wq_logid=1558343258.454457644; wqmnx1=MDEyNjM4M3Btb2M3Y3MyJTQxRjA2NWdjNEZFMjAxMU9EQjlFMDZvLm5pU2lPcHQxTEdlMmw4LzVmVTJWTykoKQ%3D%3D; wxa_level=1; unpl=V2_ZzNtbUVUQEUlWxFQeU1UBGICQQgSVBMTdAAUUnpLX1E3A0BdclRCFX0UR1BnGl0UZwQZWERcQBNFCEdkeBBVAWMDE1VGZxBFLV0CFSNGF1wjU00zEQdEQiYAT1cpTVUGYlQbX0tUFxB9CkVUfkoMVmVQElxyZ0AVRQhHZHsdWAxlBhJbR15GEXMIQ1d6Gl8DZwIRbXJQcyVFC0BWfhFdNWYzE20AAx8ddgpCXHNUXAFjChBYQlFGHHAMQFR%2bGl0GZAUSXEFnQiV2; coords=%7B%22latitude%22%3A39.9118618672279%2C%22longitude%22%3A116.4547558267543%7D; shshshfpa=c57e7aa0-a9aa-df58-6012-b8ba4919ee9a-1558341780; sc_width=375; wq_area=1_2901_0%7C2; visitkey=31086395026204789; __jdv=122270672%7Cdirect%7C-%7Cnone%7C-%7C1558341748458; webp=0',
         'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Mobile/15E148 Safari/604.1',
         'Referer': 'https://item.m.jd.com/product/{}.html?cover=jfs%2Ft10621%2F326%2F3017340533%2F104723%2F68d63060%2F5cde62b1N45d54877.jpg&pps=seckill.FO4O605%3AFOFO5O1BE7B3O13O2%3AFO010413O17O1FO4O16O10066046OA653C43D0FBO3DEBDF6E84894DCA0DEED5'.format(seed.value)
        }}
        return request

    def parse_item(self, content, seed):
        result = []
        count = self.allcnt_pattern.findall(content)
        if not count:
            result.append({"skuid": seed.value, "comment": "0"})
        else:
            result.append({"skuid": seed.value, "comment": str(count[0])})
        r = (0.1563 + random.random() / 10)
        time.sleep(r)
        if result:
            return result
        else:
            return [{"seed": seed.value}]


if __name__ == "__main__":
    current_date = timeUtil.current_time()
    process_manger.kill_old_process(sys.argv[0])
    import logging
    config = {"job_name": "jdcomment"
              , "spider_num": 23
              , "retries": 3
              , "request_timeout": 5
              , "complete_timeout": 5*60
              , "sleep_interval": -1
              , "rest_time": 5
              , "write_seed": False
              , "seeds_file": "resource/month202006"
              , "dateindex": current_date
              , "mongo_config": {"addr": "mongodb://192.168.0.13:27017", "db": "jingdong",
                                 "collection": "comment" + current_date}
              , "log_config": {"level": logging.INFO, "filename": sys.argv[0] + '.logging', "filemode":'a', "format":'%(asctime)s - %(filename)s - %(processName)s - [line:%(lineno)d] - %(levelname)s: %(message)s'}
              }
    p = GetComment(**config)
    p.main_loop(show_process=True)
