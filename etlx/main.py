# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: main.py
@time: 2021/4/27 2:35 下午
"""

from loguru import logger
import click
from datetime import datetime
from etlx.utils import parse_conf_file, build_spark_job_instance
import constant as c


@click.command()
@click.option('--config_file', help='Configuration file')
def run(config_file):
    conf = parse_conf_file(config_file)
    job_type = conf["job_type"]
    start = datetime.now()

    logger.info("作业开始: " + start.strftime("%Y-%m-%d %H:%M:%S"))

    if job_type == c.SPARK:
        source_config = conf["job_desc"]["source"]
        source = build_spark_job_instance(source_config)

    if job_type == c.PRESTO:
        pass

    if job_type == c.KYLIN:
        pass

    if job_type == c.SCRIPT:
        pass
