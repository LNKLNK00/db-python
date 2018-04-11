#!/usr/bin/python

import pymysql
import config.Config as Config

# 打开数据库连接
db = pymysql.connect(host=Config.mysql_host,port=Config.mysql_port,user=Config.mysql_user,passwd=Config.mysql_passwd,db=Config.mysql_db,charset=Config.mysql_charset)

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# SQL 查询语句
sql = "select * from videos limit 0,10"
try:
   # 执行SQL语句
   cursor.execute(sql)
   # 获取所有记录列表
   results = cursor.fetchall()
   for row in results:
      print (row)
except:
   print ("Error: unable to fetch data")

# 关闭数据库连接
db.close()