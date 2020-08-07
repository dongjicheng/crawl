# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html
from datetime import datetime

import logging
import time
from scrapy import signals
from scrapy.exceptions import NotConfigured

from fake_useragent import UserAgent

# -*- coding: utf-8 -*-
import itertools
import random


class CustomHeadersDownLoadMiddleware(object):

    def __init__(self):
        self.user_agent = UserAgent()

    @classmethod
    def get_http_proxy(cls):
        proxies = []
        proxies.extend(map(lambda x: ("http://u{0}:crawl@192.168.0.71:3128".format(x)), range(28)))
        #proxies.extend(map(lambda x: ("http://u{1}:crawl@192.168.0.{0}:3128".format(x[0], x[1])),
        #                  itertools.product(range(72, 79), range(30))))
        random.shuffle(proxies)
        return proxies

    def process_request(self, request, spider):
        request.headers['user-agent'] = self.user_agent.chrome
        request.headers["Connection"] = "keep-alive"
        request.headers["accept-encoding"] = "gzip, deflate, br"
        request.headers["accept-language"] = "zh-CN,zh;q=0.9"
        request.meta['proxy'] = random.choice(self.get_http_proxy())
        self.logger.info(request.headers)

    @property
    def logger(self):
        logger = logging.getLogger(__name__)
        return logging.LoggerAdapter(logger, {'middleware': self})

