#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random

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


def shuffle(iterable, buffer_size=512):
    buffer = []
    for i, v in enumerate(iterable):
        if i % buffer_size == 0:
            random.shuffle(buffer)
            for item in buffer:
                yield item
            buffer = [v]
        else:
            buffer.append(v)
    if buffer:
        random.shuffle(buffer)
        for item in buffer:
            yield item


if __name__ == "__main__":
    dict_obj={
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
    res = dict_to_object(dict_obj)
    print(res.xcc)