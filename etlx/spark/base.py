# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: base.py
@time: 2021/4/27 4:45 下午
"""
from dotmap import DotMap
from pyspark.sql import SparkSession


class Base:
    def __init__(self, config: DotMap, spark: SparkSession):
        self.config = config
        self.spark = spark

    def read(self):
        pass

    def write(self, df):
        pass
