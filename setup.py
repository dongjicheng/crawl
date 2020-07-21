#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(name = "jichengspider",
    version = "1.0.2",
    description = "jichengspider",
    author = "jicheng",
    author_email = "421798321@qq.com",
    url = "whatever",
    packages = find_packages(exclude=["immomo","bin","projects","test"]),
)