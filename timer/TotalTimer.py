#!/usr/bin/python

import os,sys,datetime

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import util.DbUtil as DbUtil
import util.LogUtil as LogUtil

dbUtil = DbUtil.DbUtil()
LOG = LogUtil.getLogger()

#统计每天PV、UV、广告点击、用户注册汇总数据
def day():
	LOG.info("汇总数据每日统计开始=============>")
	db = dbUtil.getConnect()
	try:
		now = datetime.datetime.strptime('2018-02-12', '%Y-%m-%d')
		cursor = db.cursor()
		# sql = '''SELECT DATE_FORMAT(NOW(),'%y-%m-%d %H:%i:%S')'''
		# cursor.execute(sql)
		# now = cursor.fetchone()
		for i in range(60):
			now = (now + datetime.timedelta(days=1))
			sql = '''SELECT COUNT(*) FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			pv = cursor.fetchone()

			sql = '''SELECT COUNT(*) from (SELECT DISTINCT ip_address FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')) t''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			uv = cursor.fetchone()

			sql = '''SELECT COUNT(*) FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			ad_click_count = cursor.fetchone()

			sql = '''SELECT COUNT(*) from (SELECT DISTINCT ip_address FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')) t''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			ad_click_user = cursor.fetchone()

			sql = '''SELECT COUNT(*) FROM customer_info WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			register_count = cursor.fetchone()

			sql = '''SELECT * FROM report_date_total WHERE DATE_FORMAT(report_date, '%%y-%%m-%%d') 
			= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			report = cursor.fetchone()

			if report is None :
				sql = '''INSERT INTO `report_date_total` (`pv`, `uv`, `ad_click_count`, `ad_click_user`, `register_count`, `report_date`) 
				VALUES (%s, %s, %s, %s, %s, '%s')''' % (pv[0], uv[0], ad_click_count[0], ad_click_user[0], register_count[0], now.strftime('%Y-%m-%d'))
				cursor.execute(sql)
			else:
				sql = '''UPDATE `report_date_total` SET pv = %s, uv = %s, ad_click_count = %s, ad_click_user = %s, register_count = %s 
				WHERE DATE_FORMAT(report_date, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (pv[0], uv[0], 
				ad_click_count[0], ad_click_user[0], register_count[0], now.strftime('%Y-%m-%d'))
				cursor.execute(sql)
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("汇总数据每日统计结束=============>")

#统计小时PV、UV、广告点击、用户注册汇总数据
def hour():
	LOG.info("汇总数据小时统计开始=============>")
	db = dbUtil.getConnect()
	try:
		now = datetime.datetime.strptime('2018-02-12', '%Y-%m-%d')
		cursor = db.cursor()
		# sql = '''SELECT DATE_FORMAT(NOW(),'%y-%m-%d %H:%i:%S')'''
		# cursor.execute(sql)
		# now = cursor.fetchone()
		for i in range(60):
			now = (now + datetime.timedelta(days=1))
			for j in range(24):
				hour = ''
				if j < 10 :
					hour = '0' + str(j)
				else:
					hour = str(j)

				sql = '''SELECT COUNT(*) FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				pv = cursor.fetchone()

				sql = '''SELECT COUNT(*) from (SELECT DISTINCT ip_address FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')) t''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				uv = cursor.fetchone()

				sql = '''SELECT COUNT(*) FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				ad_click_count = cursor.fetchone()

				sql = '''SELECT COUNT(*) from (SELECT DISTINCT ip_address FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')) t''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				ad_click_user = cursor.fetchone()

				sql = '''SELECT COUNT(*) FROM customer_info WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				register_count = cursor.fetchone()

				sql = '''SELECT * FROM report_hour_total WHERE DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (now.strftime('%Y-%m-%d ') + hour)
				cursor.execute(sql)
				report = cursor.fetchone()

				if report is None :
					sql = '''INSERT INTO `report_hour_total` (`pv`, `uv`, `ad_click_count`, `ad_click_user`, `register_count`, `report_hour`) 
					VALUES (%s, %s, %s, %s, %s, '%s')''' % (pv[0], uv[0], ad_click_count[0], ad_click_user[0], register_count[0], now.strftime('%Y-%m-%d ') + hour)
					cursor.execute(sql)
				else:
					sql = '''UPDATE `report_hour_total` SET pv = %s, uv = %s, ad_click_count = %s, ad_click_user = %s, register_count = %s 
					WHERE DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (pv[0], uv[0], 
					ad_click_count[0], ad_click_user[0], register_count[0], now.strftime('%Y-%m-%d ') + hour)
					cursor.execute(sql)
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("汇总数据小时统计结束=============>")