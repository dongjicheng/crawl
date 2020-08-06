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
import threading
import Queue

db = pymongo.MongoClient('mongodb://127.0.0.1')['51job']

jobCol = time.strftime('job_%Y%m%d',time.localtime(time.time()))
companyCol = time.strftime('company_%Y%m%d',time.localtime(time.time()))
hunterCol = time.strftime('hunter_%Y%m%d',time.localtime(time.time()))

class UniqueError(Exception):
    pass
def download(request):
    headers = ["User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"]
    while True:
        c = pycurl.Curl()
        body = BytesIO()
        c.setopt(pycurl.TIMEOUT, 5)
        #c.setopt(pycurl.CONNECTTIMEOUT, 1)
        c.setopt(pycurl.URL, request['comURL'])
        c.setopt(pycurl.HTTPHEADER, headers)
        c.setopt(pycurl.ENCODING, 'gzip,deflate')
        c.setopt(pycurl.SSL_VERIFYPEER,0)
        c.setopt(pycurl.SSL_VERIFYHOST,0)
        #c.setopt(pycurl.WRITEHEADER, headers)
        c.setopt(pycurl.WRITEFUNCTION, body.write)
        #c.setopt(pycurl.PROXY, "http://127.0.0.1:8888")                #Fiddler
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

def producer():#生产所有的任务
    comIds = db["companyALL"].distinct("_id") #取出公司数据库id
    q = Queue.Queue(len(comIds)) #建立队列

    db[jobCol].create_index("comId") #取出已完成公司信息
    items = db[jobCol].aggregate(
        [
            {
                "$group": {
                    "_id": "$comId", 
                    "hidTotal": {
                        "$max": "$hidTotal"
                    }, 
                    "pageno": {
                        "$max": "$pageno"
                    }
                }
            }, 
            {
                "$project": {
                    "complete": {
                        "$lte": [
                            "$hidTotal", 
                            {
                                "$multiply": [
                                    "$pageno", 
                                    20
                                ]
                            }
                        ]
                    }
                }
            }, 
            {
                "$match": {
                    "complete": True
                }
            }
        ])

    complete = set()
    map(lambda item:complete.add(item["_id"]),items) #循环存入已完成公司信息到jobs={}中
    for comId in comIds: 
        if comId in complete:
            continue

        pageno = 0
        hidTotal = 100
        q.put((comId, pageno, hidTotal)) #向列队中传入3个变量

    return q

class consumer(threading.Thread): #消费者执行任务方式
    def __init__(self, queue): #初始化
        super(consumer, self).__init__()
        self._q = queue

    def run(self): #运行
        while True:#取公司id
            comId,pageno,hidTotal = self._q.get() #取任务
            jobIds = set()

            comURL = "https://jobs.51job.com/all/co{}.html".format(comId)          
            while pageno * 20 < hidTotal: #翻页
                 try:
                    pageno += 1
                    formdata = { #传入请求
                        "pageno":pageno,
                        "hidTotal":hidTotal,
                        "type":"",
                        "code":""
                    }
                    task = {"comURL":comURL,"formdata":formdata}
                    content = download(task)
                    html = etree.HTML(content.decode("gb18030")) #网页解码
                    if html is None:#记录公司没有职位
                        hidTotal = 0                                
                        db[jobCol].save({
                            "comId": comId,
                            "pageno":pageno,
                            "hidTotal":0,
                            "msg":"empty"
                        })
                        break

                    result = []
               
                    for dl in html.xpath("//div[@class=\"el\"]"): #提取网页数据信息
                        jobURL = dl.xpath("./p/a/@href")[0]
                        jobId = re.search("(\d+)\.",jobURL).group(1)
                        #if jobId in jobIds:
                        #    raise UniqueError("Unique Error comId {}".format(comId)) #将错误公司id抛出，显示抛出信息
                        jobIds.add(jobId) #将跑过的职位id存入jobIds中
                        background = dl.xpath("./span[1]/text()")
                        location = dl.xpath("./span[2]/text()")
                        salary = dl.xpath("./span[3]/text()")
                        postdate = dl.xpath("./span[4]/text()")
                        hidTotal = int(dl.xpath("//*[@id=\"hidTotal\"]/@value")[0])
                        if background: #判断职位内无内容情况
                            background = background[0]
                        else:
                            background = None

                        if location:
                            location = location[0]
                        else:
                            location = None

                        if salary:
                            salary = salary[0]
                        else:
                            salary = None

                        if postdate:
                            postdate = postdate[0]
                        else:
                            postdate = None

                        result.append({
                            "comId": comId,
                            "jobURL":jobURL,
                            "jobId": jobId,
                            "hidTotal": hidTotal,
                            "pageno": pageno,
                            "position": dl.xpath("./p/a/text()")[0],
                            "backgroud": background,
                            "location": location,
                            "salary": salary,
                            "postdate": postdate,
                            })

                    if result:
                        db[jobCol].insert(result)
                    else:
                        break

                    bk = html.xpath("//li[@class=\"bk\"][last()]/a") #判断是否还有下一页，翻页判断
                    if not bk:
                        break

                 except UniqueError as ex:#公司职位更新重跑
                    print(comId,ex)
                    #db[jobCol].remove({"comId":comId})#移除原错误数据
                    #q.put((comId,0,100)) #将公司放进任务队列
                    #break
                 except Exception as  ex:#捕获异常
                     print(comId,ex)
                     db[jobCol].save({"comId": comId,"msg":ex.message})
                     raise ex #展示异常
                     break

            if hidTotal != int(len(jobIds)):
                print(comId,"hidTotal error")
                db[jobCol].remove({"comId":comId})#移除原错误数据
                q.put((comId,0,100)) #将公司放进任务队列
            else:
                q.task_done() #报告任务完成

class ThreadMonitor(threading.Thread): #进程跟踪线程
    def __init__(self,q):
        threading.Thread.__init__(self)
        self._q = q

    def run(self):
        prevTime = 0
        prevCount = maxCount = self._q.qsize() #qsize队列中剩余任务量
        while True:
            dt = time.time() - prevTime
            end = time.time() + 30

            print('%s Download %d comURLs (at %.2f comURL/min), Queue %d tasks' % \
                    (time.strftime('%Y-%m-%d %H:%M:%S'), maxCount-self._q.qsize(), 60*(prevCount - self._q.qsize())/dt, self._q.qsize()))
            prevCount = self._q.qsize()
            prevTime = time.time()

            if time.time() < end:
                threading._sleep(end - time.time())

if __name__ == '__main__':
    #db[jobCol].drop()
    q = producer()
    #q = Queue.Queue()
    #q.put((1175665,0,100))
    #1175665，3820569，3893190，4098951，4645738 ，4845307
 

    t = ThreadMonitor(q)
    t.setDaemon(True)  
    t.start() #启动线程
    count = q.qsize()
    for i in range(10): #建立10个线程
        t = consumer(q)
        t.setDaemon(True)
        t.start() 
        #break
    q.join() #当所有任务完成后停止
