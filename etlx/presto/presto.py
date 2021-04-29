# coding=utf-8
"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: presto.py
@time: 2021/4/29 5:04 下午
"""

import presto
import sqlparse
from loguru import logger


def run(job):
    host, port, user, db_name, sql = job.host, job.port, job.user, job.name, job.sql
    logger.info("presto信息: ")
    logger.info("host: " + host)
    logger.info("user: " + user)
    logger.info("db: " + db_name + '\n')

    with presto.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=db_name
    ) as conn:
        cur = conn.cursor()

        sql_list = [s.strip()[:-1] if s.strip().endswith(";") else s.strip() for s in
                    sqlparse.split(sql)]
        for sql in sql_list:
            logger.info("\n执行sql: \n" + sql)
            cur.execute(sql)
            cur.fetchone()
