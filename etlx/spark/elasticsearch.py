# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: elasticsearch.py
@time: 2021/4/27 4:47 下午
"""

from etlx.spark.base import Base
from loguru import logger


class ElasticSearch(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        pass

    def write(self, df):
        if self.config.save_mode == 'UPSERT':
            logger.info("es地址: " + self.config.target_host)
            logger.info("esIndex: " + self.config.table)
            logger.info("docId: " + self.config.source_tbl_id_col)
            logger.info("开始增量更新...")
            df.write \
                .mode("append") \
                .format("es") \
                .option("es.index.auto.create", False) \
                .option("es.nodes", self.config.target_host) \
                .option("es.mapping.id", self.config.source_tbl_id_col) \
                .option("es.write.operation", "upsert") \
                .save(self.config.table)
        else:
            logger.info("es地址: " + self.config.target_host)
            logger.info("esIndex: " + self.config.table)
            logger.info("开始全量覆盖...")
            df.write \
                .mode("overwrite") \
                .format("es") \
                .option("es.nodes", self.config.target_host) \
                .save(self.config.table)
