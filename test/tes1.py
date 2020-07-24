#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
def check(str_phone_num):
    if len(str_phone_num) != 11:
        return False
    reg = re.compile(r'^\d{11,11}$')
    print(reg.findall(str_phone_num))

print(check("18801458559"))