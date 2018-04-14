#!/usr/bin/python

import os,sys,datetime

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import util.DbUtil as DbUtil
import util.LogUtil as LogUtil

dbUtil = DbUtil.DbUtil()
LOG = LogUtil.getLogger()

#广告点击记录每日汇总
def day():
	LOG.info("广告点击记录每日汇总开始=============>")
	db = dbUtil.getConnect()
	try:
		cursor = db.cursor()
		sql = '''SELECT DATE_FORMAT(NOW(),'%Y-%m-%d %H')'''
		cursor.execute(sql)
		now_str = cursor.fetchone()[0]
		now = datetime.datetime.strptime(now_str, '%Y-%m-%d %H')
		count = 1
		if now_str[11:13] == '00':
			now = (now + datetime.timedelta(days=-2))
			count = 2
		else:
			now = (now + datetime.timedelta(days=-1))

		for i in range(count):
			now = (now + datetime.timedelta(days=1))

			sql = '''SELECT u.ad_id, p.pv, u.uv
					FROM
					(SELECT ad_id,count(*) as uv
					FROM
					(SELECT ad_id,ip_address FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d')
					GROUP BY ad_id,ip_address) t 
					GROUP BY ad_id) u
					INNER JOIN
					(SELECT ad_id,count(*) as pv FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d')
					GROUP BY ad_id) p
					ON u.ad_id = p.ad_id''' % (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				sql = '''SELECT * FROM report_date_ad_click WHERE ad_id = %s and DATE_FORMAT(report_date, '%%y-%%m-%%d') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (row[0], now.strftime('%Y-%m-%d'))
				cursor.execute(sql)
				report = cursor.fetchone()

				if report is None :
					sql = '''INSERT INTO `report_date_ad_click` (`ad_id`, `ad_click_count`, `ad_click_user`, `report_date`) 
					VALUES (%s, %s, %s, '%s')''' % (row[0], row[1], row[2], now.strftime('%Y-%m-%d'))
					cursor.execute(sql)
				else:
					sql = '''UPDATE `report_date_ad_click` SET ad_click_count = %s, ad_click_user = %s WHERE ad_id = %s and 
					DATE_FORMAT(report_date, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (row[1], row[2], 
					row[0], now.strftime('%Y-%m-%d'))
					cursor.execute(sql)
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("广告点击记录每日汇总结束=============>")

#广告点击记录小时汇总
def hour():
	LOG.info("广告点击记录小时汇总开始=============>")
	db = dbUtil.getConnect()
	try:
		cursor = db.cursor()
		sql = '''SELECT DATE_FORMAT(NOW(),'%Y-%m-%d %H')'''
		cursor.execute(sql)
		now_str = cursor.fetchone()[0]
		now = datetime.datetime.strptime(now_str, '%Y-%m-%d %H')
		now = (now + datetime.timedelta(hours=-2))

		for i in range(2):
			now = (now + datetime.timedelta(hours=1))

			sql = '''SELECT u.ad_id, p.pv, u.uv
					FROM
					(SELECT ad_id,count(*) as uv
					FROM
					(SELECT ad_id,ip_address FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					GROUP BY ad_id,ip_address) t 
					GROUP BY ad_id) u
					INNER JOIN
					(SELECT ad_id,count(*) as pv FROM log_ad_click WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					GROUP BY ad_id) p
					ON u.ad_id = p.ad_id''' % (now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'))
			cursor.execute(sql)
			rows = cursor.fetchall()

			for row in rows:
				sql = '''SELECT * FROM report_hour_ad_click WHERE ad_id = %s and DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (row[0], now.strftime('%Y-%m-%d %H'))
				cursor.execute(sql)
				report = cursor.fetchone()
				
				if report is None :
					sql = '''INSERT INTO `report_hour_ad_click` (`ad_id`, `ad_click_count`, `ad_click_user`, `report_hour`) 
					VALUES (%s, %s, %s, '%s')''' % (row[0], row[1], row[2], now.strftime('%Y-%m-%d %H'))
					cursor.execute(sql)
				else:
					sql = '''UPDATE `report_hour_ad_click` SET ad_click_count = %s, ad_click_user = %s WHERE ad_id = %s and 
					DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (row[1], row[2], 
					row[0], now.strftime('%Y-%m-%d %H'))
					cursor.execute(sql)
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("广告点击记录小时汇总结束=============>")