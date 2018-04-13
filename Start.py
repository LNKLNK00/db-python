#!/usr/bin/python

from apscheduler.schedulers.blocking import BlockingScheduler
import util.LogUtil as LogUtil
import timer.TotalTimer as TotalTimer
  
sched = BlockingScheduler() 
LOG = LogUtil.getLogger()
TotalTimer.hour()

@sched.scheduled_job('interval', seconds=3) 
def timed_job(): 
	LOG.info('每三秒执行一次任务11111111')
	print('每三秒执行一次任务11111111')
  
@sched.scheduled_job('cron', day_of_week='*', hour='15', minute='31', second='30') 
def scheduled_job(): 
	LOG.info('准时执行任务')

try:
	#sched.start()
	LOG.info("项目启动完成============>")
except Exception as e:
	error = '项目启动失败! ERROR : %s' % (e)    
	LOG.error(error)