# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: mysql.py
@time: 2021/4/27 4:46 下午
"""

from loguru import logger
import datetime
import math
from decimal import Decimal
from etlx.spark.base import Base
from pyspark.sql.types import StructType


class MySQL(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        lower_bound = None
        upper_bound = None
        num_partitions = None
        column = None
        predicates = None

        url = "jdbc:mysql://{}:{}/{}?yearIsDateType=false&zeroDateTimeBehavior=convertToNull".format(
            self.config.source.host, self.config.source.port,
            self.config.source.db)
        url += "&tinyInt1isBit=false&serverTimezone=Asia/Shanghai"
        properties = {"user": self.config.source.user, "password": self.config.source.password,
                      "fetchsize": self.config.source.fetch_size}

        if self.config.source.table and self.config.source.time_type_column:
            logger.info("如果维护了时间字段则按时间切片...")
            t = "(select min({0}) as min,".format(self.config.source.time_type_column)
            t += "max({0}) as max,count(1) as cnt from {1}.{2}) as t".format(
                self.config.source.time_type_column, self.config.source.db, self.config.source.table)

            df = self.spark.read.jdbc(url, t, properties=properties)

            ret = df.first().asDict()

            if isinstance(ret.get("max"), datetime.datetime):
                max_value = ret.get("max")
                min_value = ret.get("min")
                logger.info("业务表时间最小值为 : " + str(min_value))
                logger.info("业务表时间最大值为 : " + str(max_value))
                logger.info("业务表总行数为 : " + str(ret.get("cnt")))
                num_partitions = math.ceil(float(ret.get("cnt")) / self.config.source.count_per_partitions)
                predicates = build_predicates_by_datetime(min_value, max_value, self.config.source.time_type_column,
                                                          num_partitions)

                logger.info("分片数为 : " + str(num_partitions))
                logger.info("分片为 : " + str(predicates))

            else:
                logger.info("非时间类型字段...")

        elif self.config.source.table and self.config.source.num_type_column:

            t = "(select cast(min({0}) as Decimal(24)) as min,".format(self.config.source.num_type_column)
            t += "cast(max({0}) as Decimal(24)) as max,count(1) as cnt from {1}.{2}) as t".format(
                self.config.source.num_type_column, self.config.source.db, self.config.source.table)

            df = self.spark.read.jdbc(url, t, properties=properties)

            ret = df.first().asDict()

            if ret.get("max") and isinstance(ret.get("max"), Decimal) and ret.get(
                    "cnt") > self.config.source.count_per_partitions:
                column = self.config.source.num_type_column
                lower_bound = ret.get("min")
                upper_bound = ret.get("max")
                num_partitions = math.ceil(float(ret.get("cnt")) / self.config.source.count_per_partitions)

                logger.info("业务表id最小值为 : " + str(lower_bound))
                logger.info("业务表id最大值为 : " + str(upper_bound))
                logger.info("业务表总行数为 : " + str(ret.get("cnt")))
                logger.info("分片数为 : " + str(num_partitions))

        if self.config.source.sql:
            table = '({}) as t'.format(self.config.source.sql)
        else:
            table = "{}.{}".format(self.config.source.db, self.config.source.table)

        df = self.spark.read.jdbc(url, table,
                                  column=column,
                                  lowerBound=lower_bound,
                                  upperBound=upper_bound,
                                  numPartitions=num_partitions,
                                  predicates=predicates,
                                  properties=properties)

        return df

    def run_target_sqls(self, sqls):
        import pymysql.cursors

        connection = pymysql.connect(host=self.config.target.host,
                                     user=self.config.target.user,
                                     password=self.config.target.password,
                                     db=self.config.target.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                for sql in list(filter(lambda x: x, sqls.split(";"))):
                    logger.info("开始执行SQL: " + sql)
                    cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()

    def write(self, df):

        if self.config.target.before_sqls:
            self.run_target_sqls(self.config.target.before_sqls)

        if self.config.target.save_mode == 'UPSERT':
            batch_size = self.config.target.batch_size
            fields = df.schema.fieldNames()
            ids = self.config.source.num_type_column.split(",")
            logger.info("目标库 : " + self.config.target.host)
            logger.info("目标表 : " + self.config.target.table)
            logger.info("开始upsert更新...")
            df.coalesce(16).foreachPartition(
                upsert_wrapper(self.config.target.host, self.config.target.db, self.config.target.password,
                               self.config.target.db,
                               self.config.target.is_drds, self.config.target.table, fields, ids, batch_size))

        else:
            jdbc_url = "jdbc:{}://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8".format(self.config.target.type,
                                                                                           self.config.target.host,
                                                                                           self.config.target.port,
                                                                                           self.config.target.db)

            properties = {"user": self.config.target.db, "password": self.config.target.password}

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

        if self.config.target.after_sqls:
            self.run_target_sqls(self.config.target.after_sqls)


def upsert_wrapper(target_host, target_db_user, target_db_psw, target_db_name, drds, table_name, fields, ids,
                   batch_size):
    def upsert(rows):
        import pymysql.cursors

        connection = pymysql.connect(host=target_host,
                                     user=target_db_user,
                                     password=target_db_psw,
                                     db=target_db_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:

                i = 0

                # 如果目标表是drds，开启分布式事务
                if drds:
                    cursor.execute("SET drds_transaction_policy = 'FLEXIBLE'")

                for row in rows:
                    sql = 'INSERT INTO {} '.format(table_name)
                    sql += '(' + (',').join(fields) + ') '
                    sql += 'VALUES (' + ",".join(["%s" for x in fields]) + ") "
                    sql += 'ON DUPLICATE KEY UPDATE ' + ",".join(
                        [x + "=%s" for x in list(filter(lambda x: x not in ids, fields))])
                    cursor.execute(sql, [
                        row.__getattr__(x).__str__() if row.__getattr__(
                            x).__str__() != 'None' else None for x in fields]
                                   +
                                   [row.__getattr__(x).__str__() if row.__getattr__(
                                       x).__str__() != 'None' else None for x in
                                    list(filter(lambda x: x not in ids, fields))])

                    i = i + 1
                    if i % batch_size == 0:
                        connection.commit()
                        logger.info("已提交: {}条！".format(str(i)))
                connection.commit()
                logger.info("已提交: {}条！".format(str(i)))
        finally:
            connection.close()

    return upsert


def build_predicates_by_datetime(min_value, max_value, column, num_partitions):
    max_value_int = max_value.timestamp()
    min_value_int = min_value.timestamp()

    step_size = (max_value_int - min_value_int) / num_partitions
    logger.info("步长: " + str(step_size))
    predicates = []
    st = min_value_int
    end_str = ''
    for i in range(num_partitions - 1):
        ed = st + step_size
        start_str = datetime.datetime.fromtimestamp(st).strftime("%Y-%m-%d %H:%M:%S.%f")
        end_str = datetime.datetime.fromtimestamp(ed).strftime("%Y-%m-%d %H:%M:%S.%f")
        where_str = [f"""{column} >= '{start_str}' and {column} < '{end_str}'"""]
        predicates += where_str
        st = ed

    predicates += [f"""{column} >= '{end_str}'"""]

    return predicates


if __name__ == '__main__':
    start = datetime.datetime.strptime("1970-01-01 00:00:00.00", '%Y-%m-%d %H:%M:%S.%f')
    end = datetime.datetime.strptime("2021-01-13 14:07:07.00", '%Y-%m-%d %H:%M:%S.%f')
    r = build_predicates_by_datetime(start, end, "create_time", 21)
    logger.info(r)
