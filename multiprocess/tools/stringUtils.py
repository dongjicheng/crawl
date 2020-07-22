#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re


def check_legality(self, pattern):
    regex = re.compile(pattern)

    def is_valid(input_str):
        if regex.findall(input_str):
            return True
        else:
            return False
    return is_valid
