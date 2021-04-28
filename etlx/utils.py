# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: utils.py
@time: 2021/4/27 2:42 下午
"""

import yaml
from loguru import logger
from dotmap import DotMap

import etlx.constant as c
from etlx.spark.elasticsearch import ElasticSearch
from etlx.spark.hive import Hive
from etlx.spark.kafka import Kafka
from etlx.spark.kudu import Kudu
from etlx.spark.mssql import MSSQL
from etlx.spark.mysql import MySQL
from etlx.spark.phoenix import Phoenix
from etlx.spark.postgresql import PostgreSQL


def parse_conf_file(file_name):
    try:
        with open(file_name) as f:
            config = yaml.safe_load(f)
            dt = DotMap(config)
            return dt
    except Exception as e:
        logger.info("解析配置文件出错...")
        raise e


def build_spark_job_instance(spark_job_type, config):
    if spark_job_type == c.ELASTICSEARCH:
        return ElasticSearch(config)
    if spark_job_type == c.HIVE:
        return Hive(config)
    if spark_job_type == c.KAFKA:
        return Kafka(config)
    if spark_job_type == c.KUDU:
        return Kudu(config)
    if spark_job_type == c.MYSQL:
        return MySQL(config)
    if spark_job_type == c.MSSQL:
        return MSSQL(config)
    if spark_job_type == c.PHOENIX:
        return Phoenix(config)
    if spark_job_type == c.POSTGRESQL:
        return PostgreSQL(config)
