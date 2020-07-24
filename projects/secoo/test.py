#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np

from multiprocessing import Process
import os

import re
import json
block_pattern = re.compile(r'{"isShow.*?}')
data="""{"isShow":1,"id":1916158,"shareCount":0,"isAnonymous":"0","isPraise":0,"source":"3","productSpec":"柳叶与琥珀",
"userName":"oj15460741585","createDate":1558065846000,"grade":5,"productId":47336188,"productName":"【19春夏】【包税】JO MALONE\/祖马龙 2019年春季限量版香水30ml",
"content":"如果有包装袋就好了","imgs":"","sourceDevice":"寺库Iphone客户端",
"userImg":"https:\/\/pic12.secooimg.com\/thumb\/120\/120\/pic1.secoo.com\/headImage\/19\/3\/2c08bf43055a424a90a9f71f8b924afe.jpeg","type":1,"praiseCount":0},
{"isShow":1,"id":1843777,"shareCount":0,"isAnonymous":"0","isPraise":0,"source":"3","productSpec":"羽扇豆与广藿香","userName":"kellyvic119","createDate":1555590776000,"grade":5,"productId":47336174,"productName":"【包税】JO MALONE\/祖马龙 2019年春季限量版香水30ml","content":"好闻，不错，喜欢","sourceDevice":"寺库Iphone客户端","imgs":"","type":1,"praiseCount":0},{"isShow":1,"id":1839306,"shareCount":0,"isAnonymous":"0","isPraise":0,"source":"3","productSpec":"羽扇豆与广藿香","userName":"249681581386","createDate":1555422859000,"grade":5,"productId":47336174,"productName":"【包税】JO MALONE\/祖马龙 2019年春季限量版香水30ml","content":"送人的～～ 不错","imgs":"","sourceDevice":"寺库Iphone客户端","userImg":"https:\/\/pic12.secooimg.com\/thumb\/120\/120\/pic1.secoo.com\/headImage\/17\/6\/80360fd6975247a187398aaf30804fc8.jpeg","type":1,"praiseCount":0}],"productGrade":5,"retMsg":"查询成功"}}"""
print(json.loads(data))