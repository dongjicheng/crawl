#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymongo
import time
import json

# 先更新id库在更新companyALL_2 lib


class UpdateCompanyAll2:

    def __init__(self):
        self.db = pymongo.MongoClient('mongodb://localhost')['liepin']
        #self.companyCol = time.strftime('company_%Y%m%d',time.localtime(time.time()))
        self.companyCol = 'company_20200617'
        self.collection_newcompany = self.db[self.companyCol]
        self.collection_company_all = self.db['companyALL_2']

    def get_new_companyids(self):
        new_ids = set(self.collection_newcompany.distinct('id'))
        old_ids = set(self.collection_company_all.distinct('_id'))
        insert_ids = list(new_ids - old_ids)
        return insert_ids

    def insert_into_companyALL2(self):

        insert_ids = self.get_new_companyids()
        print('total ids -> {}'.format(len(insert_ids)))
        new_company = []
        for cid in insert_ids:
            c = self.collection_newcompany.find_one({'id': cid})
            temp = {
                '_id': c['id'],
                'name': c['name'],
                'industry': c['formdata']['industry'],
                'e_kind': c['e_kind'],
                'dq': c['formdata']['dq']
                }
            #print(temp)
            new_company.append(temp)
        self.collection_company_all.insert_many(new_company)
        print('total -> {}'.format(len(new_company)))


if __name__ == '__main__':
    u = UpdateCompanyAll2()
    u.insert_into_companyALL2()
