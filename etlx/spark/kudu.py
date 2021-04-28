# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: kudu.py
@time: 2021/4/27 4:47 下午
"""

from etlx.spark.base import Base


class Kudu(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        df = self.spark.read \
            .option("kudu.master", self.config.source.host) \
            .option("kudu.table", 'ods_{}.sync_{}'.format(self.config.source.db, self.config.source.table)) \
            .format("kudu") \
            .load()
        return df

    def write(self, df):
        df.write \
            .mode("append") \
            .option("kudu.master", self.config.target.host) \
            .option("kudu.table", self.config.target.table) \
            .format("kudu") \
            .save()
