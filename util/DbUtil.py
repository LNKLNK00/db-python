#!/usr/bin/python

import os,sys,pymysql

# 得到绝对路径
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import config.Config as Config
import util.LogUtil as LogUtil

class DbUtil(object):

	LOG = LogUtil.getLogger()

	def __init__(self):
		self.LOG.info("数据库操作工具类初始化完成")

	def getConnect(self):
		self.LOG.info("获取数据库连接")
		try:
			return pymysql.connect(host=Config.mysql_host,port=Config.mysql_port,user=Config.mysql_user,passwd=Config.mysql_passwd,db=Config.mysql_db,charset=Config.mysql_charset)        
		except Exception as e:
			error = '数据库连接异常! ERROR : %s' % (e)
			self.LOG.error(error)    
			return None

	def dbClose(self,db):
		self.LOG.info("关闭数据库连接")
		if db:
			db.close()

	def _query(self,sql=''):
		self.LOG.info("执行查询语句，SQL:" + sql)
		db = self.getConnect()
		try:
			cursor = db.cursor()
		   # 执行SQL语句
			cursor.execute(sql)
		   # 获取所有记录列表
			results = cursor.fetchall()
			return results
		except Exception as e:
			error = '数据查询异常! ERROR : %s' % (e) 
			self.LOG.error (error)
		finally:
			self.dbClose(db)