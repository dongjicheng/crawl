#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib import parse
from datetime import datetime
import random
import time
import itertools
import queue
import re
import os

import requests
import pymongo
import lxml.html


class PaginateException(BaseException):
    # download params error
    pass


class LiePin:

    def __init__(self):
        self.base_url = 'https://www.liepin.com/zhaopin/'
        self.db = pymongo.MongoClient('mongodb://192.168.10.1')['liepin']
        self.companyCol = time.strftime('company_%Y%m%d', time.localtime(time.time()))
        self.task_q = queue.Queue()
        self.reg_cmp_id = re.compile('.+company/(\d+)/')

    def download(self, task):
        """
        task: type -> dict
        cur_page(当前页): type -> int
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
            url, city_code, indus_code, cur_page = task['url'], task['city_code'], task['indus_code'], task['cur_page']
            # curPage: 爬虫页面控制参数，从0开始
            # d_pageSize=40 控制页面数量
            params = {'industries': indus_code, 'dqs': city_code, 'curPage': cur_page, 'd_pageSize': 40,
                      'fromSearchBtn': 2}
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.encoding = 'utf8'
            print('current_url -> {}'.format(response.url))
            time.sleep(random.uniform(1, 1.5))
            return response
        except Exception as e:
            # request 发生异常则将task重新放入队列中
            with open('./logs/http_error_logs', 'a') as g:
                g.write('{}\t{}\t{}\n'.format(str(e), task, time.ctime()))
            self.task_q.put(task)
            time.sleep(60)

    def html_page_parse(self, page, task):
        """
        在page中循环
        """
        result_list = []
        xml = lxml.html.fromstring(page)
        # 页面解析
        company_dom_list = xml.xpath(
            '//ul[@class="sojob-list"]/li/div[contains(@class, "sojob-item-main")]/div[contains(@class, "company-info")]')
        for dom in company_dom_list:
            company_name = dom.xpath('./p[@class="company-name"]/a/text()')
            company_url = dom.xpath('./p[@class="company-name"]/a/@href')
            try:
                company_id = self.reg_cmp_id.search(company_url[0]).group(1)
                company_info = {
                    'name': company_name[0].strip(),
                    'id': company_id,
                    'e_kind': '',
                    'industry': '',
                    'formdata': {'dq': task['city_code'], 'industry': task['indus_code']}
                }
                result_list.append(company_info)
            except IndexError:
                # 无公司id
                pass
        return result_list

    def get_url_params(self):
        # 按行业分类
        industries = []
        # 按地区分类
        cities = self.db.city.find_one({}, {"relations": 1})["relations"].keys()
        indus_query_result = self.db.industry.find_one({}, {"relations": 1})["relations"].values()
        [industries.extend(item) for item in indus_query_result]
        # 分类进行排列组合 count: 6936
        categories = itertools.product(cities, industries)
        for cate in categories:
            # 将任务放入队列中
            task = {'url': self.base_url, 'city_code': cate[0], 'indus_code': cate[1], 'cur_page': 0}
            self.task_q.put(task)
        # 获取队列任务数
        task_count = self.task_q.qsize()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('{now} create {task_count} tasks'.format(now=now, task_count=task_count))

    def insert_into_mongo(self, items):
        self.db[self.companyCol].insert(items)

    def adsl(self):
        os.system('echo "jq123" | sudo -S poff')
        print('poff')
        time.sleep(5)
        os.system('pon dsl-provider')
        print('pon')
        time.sleep(10)

    def spider_start(self):
        # 加载任务
        self.get_url_params()
        while not self.task_q.empty():
            task = self.task_q.get()
            response = self.download(task)
            if not response:
                continue
            # 判断是否有下一页进行添加
            if ('没有找到符合条件的职位' in response.text) or task['cur_page'] >= 100:
                pass
            elif 'warning-msg' in response.text:
                self.adsl()
                self.task_q.put(task)
            else:
                task['cur_page'] += 1
                self.task_q.put(task)
            result_list = self.html_page_parse(response.text, task)
            # self.insert_into_mongo(result_list)
            # print(result_list)
            # 每页写入一次
            with open('liepin_data', 'a') as g:
                for company in result_list:
                    g.write('{}\t{}\t{}\t{}\n'.format(company['id'], company['name'], company['formdata']['dq'],
                                                      company['formdata']['industry']))
                    g.flush()


if __name__ == '__main__':
    spider = LiePin()
    spider.spider_start()