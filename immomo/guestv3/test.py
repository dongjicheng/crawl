import pandas
import time
import ctypes
from multiprocessing import Process, Manager, Value, Lock
import pandas as pd
import random
import pymongo

client = pymongo.MongoClient('mongodb://192.168.0.13')
db = client['immomo']

items = db["2019Q1_totalgap"].find({},{"_id":0}).limit(10)

frame = pandas.DataFrame(items)
print frame.sum()