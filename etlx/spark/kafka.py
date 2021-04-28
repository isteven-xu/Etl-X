# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: kafka.py
@time: 2021/4/27 4:47 下午
"""
from etlx.spark.base import Base
from pyspark.sql.functions import to_json, struct
from loguru import logger


class Kafka(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        logger.info("kafka topic: " + self.config.source.table)
        df = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.source.host) \
            .option("subscribe", self.config.source.table) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        return df

    def write(self, df):
        logger.info("正在写入kafka topic: " + self.config.target.table)
        df.select(to_json(struct([df[x] for x in df.columns])).alias("value")) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.target.host) \
            .option("topic", self.config.target.table) \
            .save()
