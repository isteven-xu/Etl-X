# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: postgresql.py
@time: 2021/4/27 4:46 下午
"""

from loguru import logger
from etlx.spark.base import Base
from pyspark.sql.types import StructType


class PostgreSQL(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        jdbc_url = "jdbc:postgresql://{}:{}/{}".format(self.config.source.host, self.config.source.port,
                                                       self.config.source.db)
        properties = {"user": self.config.source.user, "password": self.config.source.password}

        df = self.spark.read.jdbc(jdbc_url, '{}'.format(self.config.source.table),
                                  properties=properties)

        return df

    def write(self, df):
        jdbc_url = "jdbc:{}://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8".format(self.config.target.type,
                                                                                       self.config.target.host,
                                                                                       self.config.target.port,
                                                                                       self.config.target.db)

        properties = {"user": self.config.target.user, "password": self.config.target.password}

        # struct array等转为string
        schema = df.schema.jsonValue()
        map(lambda x: x.update(type='string') if isinstance(x.get("type"), dict) else x,
            schema.get("fields"))
        struct = StructType.fromJson(schema)

        new_df = self.spark.createDataFrame(df.rdd, struct)
        logger.info("jdbc目标库 : {}/{}".format(self.config.target.host, self.config.target.db))
        logger.info("jdbc目标表 : {}".format(self.config.target.table))
        logger.info('jdbc线程数 : 4')
        logger.info('jdbc开始写入...')
        new_df.repartition(4) \
            .write \
            .mode("overwrite") \
            .option("truncate", True) \
            .jdbc(jdbc_url, self.config.target.table, properties=properties)
        logger.info('jdbc写入成功！')
