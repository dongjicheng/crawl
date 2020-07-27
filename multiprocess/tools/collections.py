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


class DataSet(object):
    def __init__(self, iterable):
        self.iterable_list = [iterable]

    def shuffle(self, buffer_size=512):
        iter = self.iterable_list[-1]

        def apply(iter):
            buffer = []
            for i, v in enumerate(iter):
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
        self.iterable_list.append(apply(iter))
        return self

    def map(self, function):
        iter = self.iterable_list[-1]
        def apply(iter):
            for i in iter:
                yield function(i)
        self.iterable_list.append(apply(iter))
        return self

    def apply(self):
        return self.__iter__()

    def __next__(self):
        return next(self.iterable_list[-1])

    def __iter__(self):
        return self.iterable_list[-1]


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