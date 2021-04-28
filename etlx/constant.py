# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: constant.py
@time: 2021/4/27 3:01 下午
"""

# 配置文件字段
JOB_ID = 'job_id'
JOB_TYPE = 'job_type'

# 作业类型
SPARK = 'spark'
PRESTO = 'presto'
KYLIN = 'kylin'
SCRIPT = 'script'

# spark作业子类型
HIVE = 'hive'
ELASTICSEARCH = 'elasticsearch'
PHOENIX = 'phoenix'
MYSQL = 'mysql'
POSTGRESQL = 'postgresql'
MSSQL = 'mssql'
KUDU = 'kudu'
KAFKA = 'kafka'

# 作业名称配置
SPARK_JOB_PREFIX = 'sparkETL-'
PRESTO_JOB_PREFIX = 'presto-'
KYLIN_JOB_PREFIX = 'kylin-'
SCRIPT_JOB_PREFIX = 'script-'
