#!/usr/bin/python

import os,sys

# 得到绝对路径
a = os.path.abspath(__file__)
# 得到根目录
base_dir = os.path.dirname(os.path.dirname(a))
sys.path.append(base_dir)

import util.DbUtil as DbUtil

dbUtil = DbUtil.DbUtil()
sql = "select * from videos where id =199"
result = dbUtil._query(sql)
for row in result:
	print(row)