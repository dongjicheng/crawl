# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from pymongo import MongoClient

import logging
logger = logging.getLogger(__name__)


class MongoPipeline(object):
    def __init__(self, url):
        self.url = url

    @classmethod
    def from_crawler(cls, crawler):
        url = crawler.settings.get('MONGO_URL')
        return cls(url)

    def open_spider(self, spider):
        self.client = MongoClient(self.url)
        self.db, self.table = "jicheng", "shoujiguishudi"

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        logger.debug(item)
        #self.client[self.db][self.table].save(dict(item))


class FilePipeline(object):
    def __init__(self, url):
        self.url = url

    @classmethod
    def from_crawler(cls, crawler):
        url = crawler.settings.get('MONGO_URL')
        return cls(url)

    def open_spider(self, spider):
        self.client = MongoClient(self.url)
        self.db, self.table = "jicheng", "shoujiguishudi"
        self.f_out = open("D:\\Users\\admin\\PycharmProjects\\crawl\\scrapy_projects\\scrapy-redis\\projects\\shoujiguishudi\\shoujiguishudi\\result\\shoujiguishudi","w")
        self.table_header = ["phonenumber","province","city","company"]
        self.f_out.write("\t".join(self.table_header))

    def close_spider(self, spider):
        self.client.close()
        self.f_out.close()

    def process_item(self, item, spider):
        values = [item[key] for key in self.table_header]
        self.f_out.write("\t".join(values) + "\n")
        logger.debug(item)
