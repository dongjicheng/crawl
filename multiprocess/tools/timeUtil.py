#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import  datetime


def current_time(format="%Y%m%d"):
    return datetime.now().strftime(format)
