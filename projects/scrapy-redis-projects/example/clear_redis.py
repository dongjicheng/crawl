#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    After you run the project every time,the stats infomation in still in the redis database.

    Run this file can help you clear the stats in the redis database.
"""

import redis

# default values
REDIS_HOST = '192.168.0.117'
REDIS_PORT = 6379
STATS_KEY = 'dmoz:start_urls'
STATS_KEY = 'dmoz:items'


def clear_stats():
    server = redis.Redis(REDIS_HOST, REDIS_PORT)
    server.delete(STATS_KEY)


if __name__ == "__main__":
    clear_stats()
