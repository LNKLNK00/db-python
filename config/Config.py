#!/usr/bin/python

import configparser
import os

# 获取文件的当前路径（绝对路径）
cur_path = os.path.dirname(os.path.realpath(__file__))

# 获取config.ini的路径
config_path = os.path.join(cur_path, 'config.ini')

conf = configparser.ConfigParser()
conf.read(config_path)

system_profile = conf.get('system', 'system_profile')

profile = os.path.join(cur_path, 'config-' + system_profile + '.ini')
conf.read(profile)

mysql_host = conf.get('mysql', 'mysql_host')
mysql_port = int(conf.get('mysql', 'mysql_port'))
mysql_user = conf.get('mysql', 'mysql_user')
mysql_passwd = conf.get('mysql', 'mysql_passwd')
mysql_db = conf.get('mysql', 'mysql_db')
mysql_charset = conf.get('mysql', 'mysql_charset')
