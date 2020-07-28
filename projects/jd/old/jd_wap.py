import re
import os
import sys
import time
import datetime
from random import random
#import requests
import urllib2
import json

filename = sys.argv[1]
tag = 'allCnt'
dateindex = '2020-07-03'

starttime = datetime.datetime.now()
open('CntTimeLog', 'a').write(str(starttime) + '\n')

allcnt_pattern = re.compile(r'"CommentCount": "(\d+)"')

def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

def post_parse(pid):
    header = {
         'Host': 'wq.jd.com',
         'Connection': 'keep-alive',
         'Cookie':'__jd_ref_cls=; mba_muid=15583417484571420227102; mba_sid=15583417484721681913085719337.7; sk_history=4058079%2C; PPRD_P=UUID.15583417484571420227102-LOGID.1558343259629.952955740; __jda=122270672.15583417484571420227102.1558341748.1558341748.1558341748.1; __jdb=122270672.7.15583417484571420227102|1.1558341748; __jdc=122270672; __wga=1558343259612.1558341778580.1558341778580.1558341778580.6.1; shshshfp=7f60d4b70549973377eed5c5b675dbb1; shshshfpb=i9e%2FPQKMi0jSwADEYdV5hQA%3D%3D; shshshsID=9ca7b811c7f566ed6a017b5a67d8d030_6_1558343259935; cid=9; retina=1; wq_logid=1558343258.454457644; wqmnx1=MDEyNjM4M3Btb2M3Y3MyJTQxRjA2NWdjNEZFMjAxMU9EQjlFMDZvLm5pU2lPcHQxTEdlMmw4LzVmVTJWTykoKQ%3D%3D; wxa_level=1; unpl=V2_ZzNtbUVUQEUlWxFQeU1UBGICQQgSVBMTdAAUUnpLX1E3A0BdclRCFX0UR1BnGl0UZwQZWERcQBNFCEdkeBBVAWMDE1VGZxBFLV0CFSNGF1wjU00zEQdEQiYAT1cpTVUGYlQbX0tUFxB9CkVUfkoMVmVQElxyZ0AVRQhHZHsdWAxlBhJbR15GEXMIQ1d6Gl8DZwIRbXJQcyVFC0BWfhFdNWYzE20AAx8ddgpCXHNUXAFjChBYQlFGHHAMQFR%2bGl0GZAUSXEFnQiV2; coords=%7B%22latitude%22%3A39.9118618672279%2C%22longitude%22%3A116.4547558267543%7D; shshshfpa=c57e7aa0-a9aa-df58-6012-b8ba4919ee9a-1558341780; sc_width=375; wq_area=1_2901_0%7C2; visitkey=31086395026204789; __jdv=122270672%7Cdirect%7C-%7Cnone%7C-%7C1558341748458; webp=0',
         'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Mobile/15E148 Safari/604.1',
         'Referer': 'https://item.m.jd.com/product/{}.html?cover=jfs%2Ft10621%2F326%2F3017340533%2F104723%2F68d63060%2F5cde62b1N45d54877.jpg&pps=seckill.FO4O605%3AFOFO5O1BE7B3O13O2%3AFO010413O17O1FO4O16O10066046OA653C43D0FBO3DEBDF6E84894DCA0DEED5'.format(pid)
        }
    url = 'https://wq.jd.com/commodity/comment/getcommentlist?callback=skuJDEvalB&pagesize=10&sceneval=2&skucomment=1&score=0&sku={}&sorttype=5&page=1'.format(pid)
   
    req = urllib2.Request(url, headers = header)
    content = urllib2.urlopen(req ,timeout = 5).read()
        
    #count = allcnt_pattern.findall(content)
    return content

if __name__ == '__main__':
    fo = open(filename, 'r').readlines()
    if os.path.isfile('fail'):
        fa = open('fail', 'r').read()
        clip = int(fa)
    else:
        clip = 0
    for i in range(clip, len(fo)):
        line = fo[i].strip('\n')
        open('memory', 'w').write(str(i))
        try:
            content = post_parse(line)    
            count = allcnt_pattern.findall(content)
            #print count
            if count == []:
                open('unfinishAllCnt'+dateindex, 'a').write(line + '\t' + str(0) + '\n')
            else:
                open(tag+dateindex, 'a').write(line + '\t' + str(count[0]) + '\n')
            r = (0.1563 + random() / 10)
            time.sleep(r)       
        except:
            open('fail', 'w').write(str(i))
            #time.sleep(12)
            restart_program()


