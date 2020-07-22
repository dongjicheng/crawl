#!/usr/bin/env python3
# -*- coding: utf-8 -*-
database = [
    {
        "name": "18D_Block",
        "xcc": {
            "component": {
                "core": [],
                "platform": []
            },
        },
        "uefi": {
            "component": {
                "core": [],
                "platform": []
            },
        }
    }
]


class Dict(dict):
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__


def dict_to_object(dictObj):
    if not isinstance(dictObj, dict):
        return dictObj
    inst = Dict()
    for k, v in dictObj.items():
        inst[k] = dict_to_object(v)
    return inst


# 转换字典成为对象，可以用"."方式访问对象属性
res = dict_to_object(database[0])
print(res.xcc)