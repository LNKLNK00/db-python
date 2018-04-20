#!/usr/bin/python

from apscheduler.schedulers.blocking import BlockingScheduler
import util.LogUtil as LogUtil
import timer.TotalTimer as TotalTimer
import timer.LogViewTimer as LogViewTimer
import timer.AdClickTimer as AdClickTimer
import timer.ResourceTimer as ResourceTimer
import timer.MoveDataTimer as MoveDataTimer

sched = BlockingScheduler()
LOG = LogUtil.getLogger()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def total_day():
    TotalTimer.day()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def total_hour():
    TotalTimer.hour()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def log_view_day():
    LogViewTimer.day()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def log_view_hour():
    LogViewTimer.hour()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def ad_click_day():
    AdClickTimer.day()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def ad_click_hour():
    AdClickTimer.hour()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def video_day():
    ResourceTimer.day_video()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def video_hour():
    ResourceTimer.hour_video()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def download_day():
    ResourceTimer.day_download()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def download_hour():
    ResourceTimer.hour_download()


@sched.scheduled_job('cron', day='*', hour='*', minute='10')
def novel_day():
    ResourceTimer.day_novel()


@sched.scheduled_job('cron', day='*', hour='*', minute='5')
def novel_hour():
    ResourceTimer.hour_novel()


@sched.scheduled_job('cron', day='2', hour='4', minute='15')
def move_log_view():
    MoveDataTimer.run()


try:
    LOG.info("项目启动完成============>")
    sched.start()
except Exception as e:
    error = '项目启动失败! ERROR : %s' % (e)
    LOG.error(error)
