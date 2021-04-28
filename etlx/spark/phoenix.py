# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: phoenix.py
@time: 2021/4/27 4:47 下午
"""
from loguru import logger
from etlx.spark.base import Base


class Phoenix(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        df = self.spark.read \
            .format("phoenix") \
            .option("table", self.config.source.table) \
            .option("zkUrl", self.config.source.host) \
            .load()
        return df

    def write(self, df):
        logger.info("目标库: phoenix -> " + self.config.target.host)
        logger.info("正在写入目标表: " + self.config.target.table)
        df.write \
            .format("phoenix") \
            .mode("overwrite") \
            .option("table", self.config.target.table) \
            .option("zkUrl", self.config.target.host) \
            .save()
