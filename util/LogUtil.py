#!/usr/bin/python

import logging
import os
import time

date = time.strftime("%Y-%m-%d", time.localtime())
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 获取上级目录的绝对路径
log_dir = BASE_DIR + '/log/' + date + '.log'


def getLogger():
    handler = logging.FileHandler(log_dir, encoding='utf-8')  # 创建一个文件流并设置编码utf8
    logger = logging.getLogger()  # 获得一个logger对象，默认是root
    logger.setLevel(logging.DEBUG)  # 设置最低等级debug
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(module)s %(funcName)s Line:%(lineno)s ==> %(message)s")  # 设置日志格式
    logger.addHandler(handler)  # 把文件流添加进来，流向写入到文件
    handler.setFormatter(formatter)  # 把文件流添加写入格式
    return logger
