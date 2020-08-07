#!/usr/bin/env python
# -*- coding: utf-8 -*-
from scrapy_redis.spiders import RedisSpider
from scrapy import Request
from scrapy_redis.queue import FifoQueue
from scrapy_redis.connection import (
    from_settings,
    get_redis,
    get_redis_from_settings,
)
from scrapy.utils.project import get_project_settings
from scrapy_redis import defaults
import re
from scrapy_redis.utils import bytes_to_str
from multiprocess.scrapy_redis.spiders import JiChengSpider
from scrapy.item import Item, Field
import logging
class ShoujiSpider(JiChengSpider):
    """Spider that reads urls from redis queue (myspider:start_urls)."""
    name = 'shoujiguishudi'
    queue_cls = FifoQueue
    #redis_key = 'jdskuid:start_urls'
    #allowed_domains = ['baidu.com']
    pro_city_pattern = re.compile(r'<dd><span>号码归属地：</span>(.*?) (.*?)</dd>')
    telcompany_pattern = re.compile(r'<dd><span>手机卡类型：</span>(.*?)</dd>')

    class IItem(Item):
        _seed = Field()
        phonenumber = Field()
        province = Field()
        city = Field()
        company = Field()

    def parse(self, response):
        pro_city = self.pro_city_pattern.findall(response.text)
        tel_compay = self.telcompany_pattern.findall(response.text)
        item = self.IItem()
        item["phonenumber"] = response.meta["_seed"]
        item["province"] = pro_city[0][0]
        item["city"] = pro_city[0][0] if pro_city[0][1] == "" else pro_city[0][1]
        item["company"] = tel_compay[0]
        yield item


