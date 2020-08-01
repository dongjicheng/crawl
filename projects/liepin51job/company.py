# -*- coding: utf-8 -*-
import json
import pymongo
import pycurl
from io import BytesIO
import urllib
import time
from datetime import datetime
import itertools
from lxml import etree
import re

#db = pymongo.MongoClient('mongodb://192.168.0.13')['51job']
db = pymongo.MongoClient('mongodb://127.0.0.1')['51job']
jobCol = time.strftime('job_%Y%m%d',time.localtime(time.time()))
companyCol = time.strftime('company_%Y%m%d',time.localtime(time.time()))
hunterCol = time.strftime('hunter_%Y%m%d',time.localtime(time.time()))

def download(request):
    headers = ["User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"]
    while True:
        c = pycurl.Curl()
        body = BytesIO()
        c.setopt(pycurl.TIMEOUT, 5)
        #c.setopt(pycurl.CONNECTTIMEOUT, 1)
        c.setopt(pycurl.URL, request['url'])
        c.setopt(pycurl.HTTPHEADER, headers)
        c.setopt(pycurl.ENCODING, 'gzip,deflate')
        c.setopt(pycurl.SSL_VERIFYPEER,0)
        c.setopt(pycurl.SSL_VERIFYHOST,0)
        #c.setopt(pycurl.WRITEHEADER, headers)
        c.setopt(pycurl.WRITEFUNCTION, body.write)
        #c.setopt(pycurl.PROXY, "http://127.0.0.1:8888")
        #c.setopt(pycurl.PROXYUSERPWD, self.userpwd)

        if 'formdata' in request:
            postfields = urllib.urlencode(request['formdata'])
            c.setopt(pycurl.POST,1)
            c.setopt(pycurl.POSTFIELDS,postfields)
        if 'postfields' in request:
            c.setopt(pycurl.POST,1)
            c.setopt(pycurl.POSTFIELDS,request['postfields'])

        try:
            c.perform()
            code = c.getinfo(pycurl.RESPONSE_CODE)
            if code != 200:
                raise pycurl.error(code, "")
            break
        except pycurl.error as err:
            if err[0] in (7,28,56):
                continue
            else:
                print('{}, {}, {}'.format(time.strftime('%H:%M:%S'),
                                                    err[0], err[1]))
                raise err
        finally:
            c.close()
        
    return body.getvalue()

def socompany():
    db[companyCol].drop()
    dt = datetime.now()
    Curpage = 1
 
    while True:
        if (datetime.now() - dt).microseconds < 1000:
            time.sleep(1)
        dt = datetime.now()
        task = {"url":"https://company.51job.com/p{}/".format(Curpage)}
        content = download(task)
        html = etree.HTML(content.decode("gbk"))
        result = []
        for dl in html.xpath("//div[@class=\"c2-main\"]/div"):
            url = dl.xpath("./span/a/@href")[0]
            id = re.search("(\d+)\.",url).group(1) 
            compkind = dl.xpath("./span[2]/text()")
            size = dl.xpath("./span[3]/text()")
            location = dl.xpath("./span[4]/text()") 
            industry = dl.xpath("./span[5]/text()")

            if compkind:
                compkind = compkind[0]
            else:
                compkind = None
            if size:
                size = size[0]
            else:
                size = None
            
            if location:
                location = location[0]
            else:
                location = None

            if industry:
                industry = industry[0]
            else:
                industry = None
            try:
                result.append({
                    "id": id,
                    "name": dl.xpath("./span[1]/a/text()")[0],
                    "compkind": compkind,
                    "size": size,
                    "location": location, 
                    "industry": industry,
                })
            except IndexError:
                print(task)
        if result:
            db[companyCol].insert(result)
        else:
            break
        if Curpage % 10 ==0:
            print(Curpage)
        pagerbar = html.xpath("//li[@class=\"bk\"][last()]/a")
        if pagerbar:
            Curpage+=1
        else:
            break


def insert2companyAll():
    new_company = []
    new_ids = set(db[companyCol].distinct('id'))
    old_ids = set(db['companyALL'].distinct('_id'))
    insert_ids = list(new_ids - old_ids)
    print(len(insert_ids))
    new_items = db['company_20200615'].find({'id': {'$in': insert_ids}})
    for item in new_items:
        co = {
            '_id': item['id'],
            'name': item['name'],
            'industry': item['industry'],
            'location': item['location'],
            'compkind': item['compkind'],
            'size': item['size']
        }
        new_company.append(co)
        # print(co)
    db['companyALL'].insert(new_company)
    print('new company insert successful!')

if __name__ == '__main__':
    socompany()
    insert2companyAll()
