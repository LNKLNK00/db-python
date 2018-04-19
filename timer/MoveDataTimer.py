#!/usr/bin/python

import os,sys,datetime,calendar

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import util.DbUtil as DbUtil
import util.LogUtil as LogUtil

dbUtil = DbUtil.DbUtil()
LOG = LogUtil.getLogger()

#月份加减
def add_months(dt,months):
	month = dt.month - 1 + months
	if month < 0 and month % 12 != 0:
		year = dt.year + int(month / 12) - 1
	else:
		year = dt.year + int(month / 12)
	month = month % 12 + 1
	day = min(dt.day,calendar.monthrange(year,month)[1])
	return dt.replace(year=year, month=month, day=day)

#log_view数据迁移，仅保留近两个月的数据
def run():
	LOG.info("log_view数据迁移开始=============>")
	db = dbUtil.getConnect()
	try:
		cursor = db.cursor()
		sql = '''SELECT DATE_FORMAT(NOW(),'%Y-%m-%d')'''
		cursor.execute(sql)
		now_str = cursor.fetchone()[0]
		now = datetime.datetime.strptime(now_str, '%Y-%m-%d')
		now = add_months(now,-2)

		sql = '''INSERT INTO log_view_old (id, ip_address, customer_id, view_type, view_id, create_time) 
		SELECT * FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-01') = 
		DATE_FORMAT('%s', '%%y-%%m-01')''' % (now.strftime('%Y-%m-01'))
		cursor.execute(sql)

		sql = '''DELETE FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-01') = 
		DATE_FORMAT('%s', '%%y-%%m-01')''' % (now.strftime('%Y-%m-01'))
		cursor.execute(sql)

		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e)
		print(error)
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("log_view数据迁移结束=============>")