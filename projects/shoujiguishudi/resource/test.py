#!/usr/bin/env python
# -*- coding: utf-8 -*-
from tqdm import tqdm
from collections import OrderedDict

total = 1000000 #总迭代次数
loss = total
with tqdm(total=total, desc="进度") as pbar:
    for i  in range(total):
        loss -= 1
#        pbar.set_postfix(OrderedDict(loss='{0:1.5f}'.format(loss)))
        pbar.set_postfix({'loss' : '{0:1.5f}'.format(loss)}) #输入一个字典，显示实验指标
        if i > 500000:
            pbar.update(-1)
        else:
            pbar.update(1)

