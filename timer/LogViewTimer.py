#!/usr/bin/python

import datetime
import os
import sys

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import util.DbUtil as DbUtil
import util.LogUtil as LogUtil

dbUtil = DbUtil.DbUtil()
LOG = LogUtil.getLogger()


# 页面访问记录每日汇总
def day():
    LOG.info("页面访问记录每日汇总开始=============>")
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

            sql = '''SELECT u.view_type, p.pv, u.uv
					FROM
					(SELECT view_type,count(*) as uv
					FROM
					(SELECT view_type,ip_address FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d')
					GROUP BY view_type,ip_address) t 
					GROUP BY view_type) u
					INNER JOIN
					(SELECT view_type,count(*) as pv FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d')
					GROUP BY view_type) p
					ON u.view_type = p.view_type''' % (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                sql = '''SELECT * FROM report_date_log_view WHERE view_type = %s and DATE_FORMAT(report_date, '%%y-%%m-%%d') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (row[0], now.strftime('%Y-%m-%d'))
                cursor.execute(sql)
                report = cursor.fetchone()

                if report is None:
                    sql = '''INSERT INTO `report_date_log_view` (`view_type`, `pv`, `uv`, `report_date`) 
					VALUES (%s, %s, %s, '%s')''' % (row[0], row[1], row[2], now.strftime('%Y-%m-%d'))
                    cursor.execute(sql)
                else:
                    sql = '''UPDATE `report_date_log_view` SET pv = %s, uv = %s WHERE view_type = %s and 
					DATE_FORMAT(report_date, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (row[1], row[2],
                                                                                                     row[0],
                                                                                                     now.strftime(
                                                                                                         '%Y-%m-%d'))
                    cursor.execute(sql)
        db.commit()
    except Exception as e:
        error = '数据操作异常! ERROR : %s' % (e)
        LOG.error(error)
        db.rollback()
    finally:
        dbUtil.dbClose(db)
        LOG.info("页面访问记录每日汇总结束=============>")


# 页面访问记录小时汇总
def hour():
    LOG.info("页面访问记录小时汇总开始=============>")
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

            sql = '''SELECT u.view_type, p.pv, u.uv
					FROM
					(SELECT view_type,count(*) as uv
					FROM
					(SELECT view_type,ip_address FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					GROUP BY view_type,ip_address) t 
					GROUP BY view_type) u
					INNER JOIN
					(SELECT view_type,count(*) as pv FROM log_view WHERE DATE_FORMAT(create_time, '%%y-%%m-%%d %%H') 
								= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					GROUP BY view_type) p
					ON u.view_type = p.view_type''' % (now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'))
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                sql = '''SELECT * FROM report_hour_log_view WHERE view_type = %s and DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') 
				= DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (row[0], now.strftime('%Y-%m-%d %H'))
                cursor.execute(sql)
                report = cursor.fetchone()

                if report is None:
                    sql = '''INSERT INTO `report_hour_log_view` (`view_type`, `pv`, `uv`, `report_hour`) 
					VALUES (%s, %s, %s, '%s')''' % (row[0], row[1], row[2], now.strftime('%Y-%m-%d %H'))
                    cursor.execute(sql)
                else:
                    sql = '''UPDATE `report_hour_log_view` SET pv = %s, uv = %s WHERE view_type = %s and 
					DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (
                    row[1], row[2],
                    row[0], now.strftime('%Y-%m-%d %H'))
                    cursor.execute(sql)
        db.commit()
    except Exception as e:
        error = '数据操作异常! ERROR : %s' % (e)
        LOG.error(error)
        db.rollback()
    finally:
        dbUtil.dbClose(db)
        LOG.info("页面访问记录小时汇总结束=============>")
