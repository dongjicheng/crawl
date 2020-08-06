#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

import pymongo


class FormatCompany:

    def __init__(self, filename):
        # format后的tablename
        self.table_name =  time.strftime('company_%Y%m%d',time.localtime(time.time()))
        self.new_company_lib = time.strftime('CompanyIdLib_%Y%m%d',time.localtime(time.time()))
        # company id库
        self.company_ids = 'CompanyIdLib_20200602'
        # 最终插入mongo的数据列表
        self.final_list = []
        # liepin_data
        self.filename = filename
        self.db = pymongo.MongoClient('mongodb://localhost:27017')['liepin']

    def read_file(self):
        """
        读取liepin_data并返回一个dict和一个set
        """
        temp_dict = {}
        temp_set = set()
        with open(self.filename, 'r') as g:
            for line in g.readlines():
                company_id, company_name, dq, indus = line.strip().split('\t')
                temp_dict.setdefault(company_id, {
                    'name': company_name,
                    'dq': dq,
                    'industry': indus
                    })
                temp_set.add(company_id)
        return temp_set, temp_dict

    def generate_new_company_ids(self, id_lib):
        """生成新的companyids库"""
        insert_dict = [{'_id': str(cid)} for cid in id_lib]
        self.db[self.new_company_lib].insert_many(insert_dict)


    def compare_and_insert(self):
        # company id 库
        company_ids = set(self.db[self.company_ids].distinct('_id'))
        new_company_ids, company_dict = self.read_file()

        # 生成新的companyidlib
        all_company_ids = company_ids.union(new_company_ids)
        self.generate_new_company_ids(all_company_ids)
        # insert ids
        # insert_ids = new_company_ids - company_ids
        # print(len(insert_ids))
        for cid in new_company_ids:
            temp = {
                'name': company_dict[cid]['name'],
                'industry': '',
                'id': cid,
                'e_kind': '',
                'formdata': {
                    'pagesize': 100,
                    'industry': company_dict[cid]['industry'],
                    'e_kind': 000,
                    'curPage': 0,
                    'keywords': '',
                    'dq': company_dict[cid]['dq']
                }
            }
            self.final_list.append(temp)
        # 插入数据
        self.db[self.table_name].insert_many(self.final_list)



if __name__ == '__main__':
    import sys
    filename = sys.argv[1]
    fc = FormatCompany(filename)
    fc.compare_and_insert()
