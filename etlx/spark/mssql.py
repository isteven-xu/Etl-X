# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: mssql.py
@time: 2021/4/27 4:47 下午
"""
from loguru import logger

from etlx.spark.base import Base


class MSSQL(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        pass

    def write(self, df):
        if self.config.target.save_mode == 'UPSERT':
            if self.config.source.pk_column.strip() == '':
                logger.info("mssql增量更新需指定主键，作业已停止")
                return
            batch_size = self.config.target.upsert_batch_size
            fields = df.schema.fieldNames()
            ids = self.config.source.pk_column.split(",")
            update_fields = list(set(fields) - set(ids))

            logger.info("目标库 : " + self.config.target.host)
            logger.info("目标表 : " + self.config.target.table)
            logger.info("开始upsert更新...")
            df.coalesce(16).foreachPartition(
                upsert_wrapper(self.config.target.host, self.config.target.user, self.config.target.password,
                               self.config.target.db, self.config.target.table, update_fields, ids, fields, batch_size))


def upsert_wrapper(target_host, target_db_user, target_db_psw, target_db_name, table_name, update_fields, ids, fields,
                   batch_size):
    def upsert(rows):
        import pymssql
        conn = pymssql.connect(target_host, target_db_user, target_db_psw,
                               target_db_name)
        try:
            with conn.cursor() as cursor:
                i = 0
                for row in rows:
                    sql = "UPDATE " + table_name + " SET "
                    sql += ",".join(
                        [(x + "='" + row.__getattr__(x).__str__() + "'"
                          if row.__getattr__(x) else x + '=null') for x in update_fields])
                    # 这里要求主键唯一字段
                    sql += " WHERE " + ids[0] + "='" + row.__getattr__(ids[0]).__str__() + "'"
                    sql += " IF @@ROWCOUNT=0 "
                    sql += "INSERT INTO " + table_name + "(" + ",".join(x for x in fields) + ") "
                    sql += "VALUES(" + ",".join(("'" + row.__getattr__(x).__str__() + "'"
                                                 if row.__getattr__(x) else "null") for x in
                                                fields) + ')'
                    cursor.execute(sql)

                    i = i + 1
                    if i % batch_size == 0:
                        conn.commit()
                        logger.info("已提交: {}条！".format(str(i)))
                conn.commit()
                logger.info("已提交: {}条！".format(str(i)))

        finally:
            conn.close()

    return upsert
