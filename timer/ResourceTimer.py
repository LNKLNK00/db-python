#!/usr/bin/python

import os,sys,datetime

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)

import util.DbUtil as DbUtil
import util.LogUtil as LogUtil

dbUtil = DbUtil.DbUtil()
LOG = LogUtil.getLogger()

#视频访问记录每日汇总
def day_video():
	LOG.info("视频访问记录每日汇总开始=============>")
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

			sql = '''SELECT
						1 AS resources_type,
						u.video_type1 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.video_type1,
								count(*) AS uv
							FROM
								(
									SELECT
										video.video_type1,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN videos video ON video.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
									AND log.view_type = 3
									GROUP BY
										video.video_type1,
										log.ip_address
								) t
							GROUP BY
								t.video_type1
						) u
					INNER JOIN (
						SELECT
							video.video_type1,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN videos video ON video.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
						AND log.view_type = 3
						GROUP BY
							video.video_type1
					) p ON u.video_type1 = p.video_type1
					UNION
						SELECT
							1 AS resources_type,
							u.video_type2 AS resources_type_id,
							p.pv,
							u.uv
						FROM
							(
								SELECT
									t.video_type2,
									count(*) AS uv
								FROM
									(
										SELECT
											video.video_type2,
											log.ip_address
										FROM
											log_view log
										LEFT JOIN videos video ON video.id = log.view_id
										WHERE
											DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
										AND log.view_type = 3
										GROUP BY
											video.video_type2,
											log.ip_address
									) t
								GROUP BY
									t.video_type2
							) u
						INNER JOIN (
							SELECT
								video.video_type2,
								COUNT(*) AS pv
							FROM
								log_view log
							LEFT JOIN videos video ON video.id = log.view_id
							WHERE
								DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
							AND log.view_type = 3
							GROUP BY
								video.video_type2
						) p ON u.video_type2 = p.video_type2''' % (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'), 
			now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_day(cursor, rows, now.strftime('%Y-%m-%d'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("视频访问记录每日汇总结束=============>")

#种子访问记录每日汇总
def day_download():
	LOG.info("种子访问记录每日汇总开始=============>")
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

			sql = '''SELECT
						2 AS resources_type,
						u.video_type1 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.video_type1,
								count(*) AS uv
							FROM
								(
									SELECT
										download.video_type1,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN downloads download ON download.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
									AND log.view_type = 5
									GROUP BY
										download.video_type1,
										log.ip_address
								) t
							GROUP BY
								t.video_type1
						) u
					INNER JOIN (
						SELECT
							download.video_type1,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN downloads download ON download.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
						AND log.view_type = 5
						GROUP BY
							download.video_type1
					) p ON u.video_type1 = p.video_type1
					UNION
						SELECT
							2 AS resources_type,
							u.video_type2 AS resources_type_id,
							p.pv,
							u.uv
						FROM
							(
								SELECT
									t.video_type2,
									count(*) AS uv
								FROM
									(
										SELECT
											download.video_type2,
											log.ip_address
										FROM
											log_view log
										LEFT JOIN downloads download ON download.id = log.view_id
										WHERE
											DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
										AND log.view_type = 5
										GROUP BY
											download.video_type2,
											log.ip_address
									) t
								GROUP BY
									t.video_type2
							) u
						INNER JOIN (
							SELECT
								download.video_type2,
								COUNT(*) AS pv
							FROM
								log_view log
							LEFT JOIN downloads download ON download.id = log.view_id
							WHERE
								DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
							AND log.view_type = 5
							GROUP BY
								download.video_type2
						) p ON u.video_type2 = p.video_type2''' % (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'), 
			now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_day(cursor, rows, now.strftime('%Y-%m-%d'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("种子访问记录每日汇总结束=============>")

#小说访问记录每日汇总
def day_novel():
	LOG.info("小说访问记录每日汇总开始=============>")
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
			
			sql = '''SELECT
						3 AS resources_type,
						u.novel_type1 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.novel_type1,
								count(*) AS uv
							FROM
								(
									SELECT
										novel.novel_type1,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN novels novel ON novel.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
									AND log.view_type = 7
									GROUP BY
										novel.novel_type1,
										log.ip_address
								) t
							GROUP BY
								t.novel_type1
						) u
					INNER JOIN (
						SELECT
							novel.novel_type1,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN novels novel ON novel.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
						AND log.view_type = 7
						GROUP BY
							novel.novel_type1
					) p ON u.novel_type1 = p.novel_type1
					UNION
						SELECT
							3 AS resources_type,
							u.novel_type2 AS resources_type_id,
							p.pv,
							u.uv
						FROM
							(
								SELECT
									t.novel_type2,
									count(*) AS uv
								FROM
									(
										SELECT
											novel.novel_type2,
											log.ip_address
										FROM
											log_view log
										LEFT JOIN novels novel ON novel.id = log.view_id
										WHERE
											DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
										AND log.view_type = 7
										GROUP BY
											novel.novel_type2,
											log.ip_address
									) t
								GROUP BY
									t.novel_type2
							) u
						INNER JOIN (
							SELECT
								novel.novel_type2,
								COUNT(*) AS pv
							FROM
								log_view log
							LEFT JOIN novels novel ON novel.id = log.view_id
							WHERE
								DATE_FORMAT(log.create_time, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')
							AND log.view_type = 7
							GROUP BY
								novel.novel_type2
						) p ON u.novel_type2 = p.novel_type2''' % (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'), 
			now.strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_day(cursor, rows, now.strftime('%Y-%m-%d'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("小说访问记录每日汇总结束=============>")

#视频访问记录小时汇总
def hour_video():
	LOG.info("视频访问记录小时汇总开始=============>")
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

			sql = '''SELECT
					1 AS resources_type,
					u.video_type1 AS resources_type_id,
					p.pv,
					u.uv
				FROM
					(
						SELECT
							t.video_type1,
							count(*) AS uv
						FROM
							(
								SELECT
									video.video_type1,
									log.ip_address
								FROM
									log_view log
								LEFT JOIN videos video ON video.id = log.view_id
								WHERE
									DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
								AND log.view_type = 3
								GROUP BY
									video.video_type1,
									log.ip_address
							) t
						GROUP BY
							t.video_type1
					) u
				INNER JOIN (
					SELECT
						video.video_type1,
						COUNT(*) AS pv
					FROM
						log_view log
					LEFT JOIN videos video ON video.id = log.view_id
					WHERE
						DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					AND log.view_type = 3
					GROUP BY
						video.video_type1
				) p ON u.video_type1 = p.video_type1
				UNION
					SELECT
						1 AS resources_type,
						u.video_type2 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.video_type2,
								count(*) AS uv
							FROM
								(
									SELECT
										video.video_type2,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN videos video ON video.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
									AND log.view_type = 3
									GROUP BY
										video.video_type2,
										log.ip_address
								) t
							GROUP BY
								t.video_type2
						) u
					INNER JOIN (
						SELECT
							video.video_type2,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN videos video ON video.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
						AND log.view_type = 3
						GROUP BY
							video.video_type2
					) p ON u.video_type2 = p.video_type2''' % (now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'), 
			now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_hour(cursor, rows, now.strftime('%Y-%m-%d %H'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("视频访问记录小时汇总结束=============>")

#种子访问记录小时汇总
def hour_download():
	LOG.info("种子访问记录小时汇总开始=============>")
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

			sql = '''SELECT
					2 AS resources_type,
					u.video_type1 AS resources_type_id,
					p.pv,
					u.uv
				FROM
					(
						SELECT
							t.video_type1,
							count(*) AS uv
						FROM
							(
								SELECT
									download.video_type1,
									log.ip_address
								FROM
									log_view log
								LEFT JOIN downloads download ON download.id = log.view_id
								WHERE
									DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
								AND log.view_type = 5
								GROUP BY
									download.video_type1,
									log.ip_address
							) t
						GROUP BY
							t.video_type1
					) u
				INNER JOIN (
					SELECT
						download.video_type1,
						COUNT(*) AS pv
					FROM
						log_view log
					LEFT JOIN downloads download ON download.id = log.view_id
					WHERE
						DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					AND log.view_type = 5
					GROUP BY
						download.video_type1
				) p ON u.video_type1 = p.video_type1
				UNION
					SELECT
						2 AS resources_type,
						u.video_type2 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.video_type2,
								count(*) AS uv
							FROM
								(
									SELECT
										download.video_type2,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN downloads download ON download.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
									AND log.view_type = 5
									GROUP BY
										download.video_type2,
										log.ip_address
								) t
							GROUP BY
								t.video_type2
						) u
					INNER JOIN (
						SELECT
							download.video_type2,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN downloads download ON download.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
						AND log.view_type = 5
						GROUP BY
							download.video_type2
					) p ON u.video_type2 = p.video_type2''' % (now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'), 
			now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_hour(cursor, rows, now.strftime('%Y-%m-%d %H'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("种子访问记录小时汇总结束=============>")

#小说访问记录小时汇总
def hour_novel():
	LOG.info("小说访问记录小时汇总开始=============>")
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

			sql = '''SELECT
					3 AS resources_type,
					u.novel_type1 AS resources_type_id,
					p.pv,
					u.uv
				FROM
					(
						SELECT
							t.novel_type1,
							count(*) AS uv
						FROM
							(
								SELECT
									novel.novel_type1,
									log.ip_address
								FROM
									log_view log
								LEFT JOIN novels novel ON novel.id = log.view_id
								WHERE
									DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
								AND log.view_type = 7
								GROUP BY
									novel.novel_type1,
									log.ip_address
							) t
						GROUP BY
							t.novel_type1
					) u
				INNER JOIN (
					SELECT
						novel.novel_type1,
						COUNT(*) AS pv
					FROM
						log_view log
					LEFT JOIN novels novel ON novel.id = log.view_id
					WHERE
						DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
					AND log.view_type = 7
					GROUP BY
						novel.novel_type1
				) p ON u.novel_type1 = p.novel_type1
				UNION
					SELECT
						3 AS resources_type,
						u.novel_type2 AS resources_type_id,
						p.pv,
						u.uv
					FROM
						(
							SELECT
								t.novel_type2,
								count(*) AS uv
							FROM
								(
									SELECT
										novel.novel_type2,
										log.ip_address
									FROM
										log_view log
									LEFT JOIN novels novel ON novel.id = log.view_id
									WHERE
										DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
									AND log.view_type = 7
									GROUP BY
										novel.novel_type2,
										log.ip_address
								) t
							GROUP BY
								t.novel_type2
						) u
					INNER JOIN (
						SELECT
							novel.novel_type2,
							COUNT(*) AS pv
						FROM
							log_view log
						LEFT JOIN novels novel ON novel.id = log.view_id
						WHERE
							DATE_FORMAT(log.create_time, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')
						AND log.view_type = 7
						GROUP BY
							novel.novel_type2
					) p ON u.novel_type2 = p.novel_type2''' % (now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'), 
			now.strftime('%Y-%m-%d %H'), now.strftime('%Y-%m-%d %H'))
			cursor.execute(sql)
			rows = cursor.fetchall()
			insert_hour(cursor, rows, now.strftime('%Y-%m-%d %H'))
		db.commit()
	except Exception as e:
		error = '数据操作异常! ERROR : %s' % (e) 
		LOG.error (error)
		db.rollback()
	finally:
		dbUtil.dbClose(db)
		LOG.info("小说访问记录小时汇总结束=============>")

#日统计数据插入
def insert_day(cursor, rows, date):
	for row in rows:
		sql = '''SELECT * FROM report_date_resource_log WHERE resource_type = %s and resource_type_id = %s 
		and DATE_FORMAT(report_date, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (row[0], row[1], date)
		cursor.execute(sql)
		report = cursor.fetchone()
		if report is None :
			sql = '''INSERT INTO `report_date_resource_log` (`resource_type`, `resource_type_id`, `pv`, `uv`,
			 `report_date`) VALUES (%s, %s, %s, %s, '%s')''' % (row[0], row[1], row[2], row[3], date)
			cursor.execute(sql)
		else:
			sql = '''UPDATE `report_date_resource_log` SET pv = %s, uv = %s WHERE resource_type = %s and 
			resource_type_id = %s and DATE_FORMAT(report_date, '%%y-%%m-%%d') = DATE_FORMAT('%s', '%%y-%%m-%%d')''' % (
			row[2], row[3], row[0], row[1], date)
			cursor.execute(sql)

#小时统计数据插入
def insert_hour(cursor, rows, hour):
	for row in rows:
		sql = '''SELECT * FROM report_hour_resource_log WHERE resource_type = %s and resource_type_id = %s 
		and DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (row[0], row[1], hour)
		cursor.execute(sql)
		report = cursor.fetchone()
		if report is None :
			sql = '''INSERT INTO `report_hour_resource_log` (`resource_type`, `resource_type_id`, `pv`, `uv`,
			 `report_hour`) VALUES (%s, %s, %s, %s, '%s')''' % (row[0], row[1], row[2], row[3], hour)
			cursor.execute(sql)
		else:
			sql = '''UPDATE `report_hour_resource_log` SET pv = %s, uv = %s WHERE resource_type = %s and 
			resource_type_id = %s and DATE_FORMAT(report_hour, '%%y-%%m-%%d %%H') = DATE_FORMAT('%s', '%%y-%%m-%%d %%H')''' % (
			row[2], row[3], row[0], row[1], hour)
			cursor.execute(sql)