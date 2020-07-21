# -*- coding: utf-8 -*-
import uuid
import hashlib
import random

def getMd5(src):
    m = hashlib.md5()
    m.update(src)
    return m.hexdigest()

def getSha1(src):
    m = hashlib.sha1()
    m.update(src)
    return m.hexdigest()

def randomMAC():
    mac = [
        random.randint(0x00, 0x7f),
        random.randint(0x00, 0x7f),
        random.randint(0x00, 0x7f),
        random.randint(0x00, 0x7f),
        random.randint(0x00, 0x7f),
        random.randint(0x00, 0x7f)]

    return ':'.join(map(lambda x: "%02x" % x, mac))

def getSessionID():
    prm_1 = str(uuid.uuid1())
    prm_2 = str(uuid.uuid1())
    uniqueid = "155699898197567" + randomMAC()
    p1 = getMd5(prm_1)[0:8]
    p2 = getMd5(prm_2)[24:]
    prm_3 = getSha1(uniqueid + p1 + p2)[10:22]
    return {
            "url":"https://api.immomo.com/guest/login/index",
            "formdata":{
                "prm_1":prm_1,
                "prm_2":prm_2,
                "prm_3":prm_3,
                "uniqueid":uniqueid,
                "uid":''.join(random.choice("1234567890abcdefghijklmnopqrstuvwxyz"))
            }
        }
