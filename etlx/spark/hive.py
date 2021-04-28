# coding=utf-8


"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: hive.py
@time: 2021/4/27 4:46 下午
"""
import sqlparse
from loguru import logger
from datetime import datetime, timedelta
import math
import string
from etlx.spark.base import Base
from pyspark.sql.functions import collect_set, count, col


class Hive(Base):
    def __init__(self, *args):
        super().__init__(*args)

    def read(self):
        ret_df = None
        # 注册外部表
        self.register_external_tables()
        # 注册udf
        self.register_udf()

        sql_list = [s.strip()[:-1] if s.strip().endswith(";") else s.strip() for s in
                    sqlparse.split(self.config.config_sql_text)]
        if not sql_list:
            raise ValueError("Sql内容为空!")
        df = None
        for sql in sql_list:
            # 改写sql
            sql = self.replace_time_vars(sql)
            df = self.spark.sql(sql)
        return df

    def write(self, df):
        TABLE_DATE_CREATE_TIME = None
        if len(df.head(1)) > 0:
            df = self.repartition(df)
            logger.info("创建数据库(如果不存在) : " + self.config.schema)
            self.spark.sql("create database if not exists {}".format(self.config.schema))
            logger.info("开始写入目标表 : " + self.config.table)
            if self.config.layer == 'ODS':
                df.write.mode("overwrite").format("orc").saveAsTable(self.config.table)
            else:
                df.write.format("orc").insertInto(self.config.table, overwrite=True)

            TABLE_DATE_CREATE_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info('目标表写入成功！')

        # hive 表进行数据质量指标记录
        try:
            metrics = self.config.target.quality
            if metrics and isinstance(metrics, list):
                dqc_start = datetime.now()
                logger.info("数据质量指标计算开始 : " + dqc_start.strftime("%Y-%m-%d %H:%M:%S"))
                values = {}
                condition = len(df.head(1)) > 0
                for metric in metrics:
                    k = metric.type
                    v = values.get(k)
                    if not v:
                        if condition:
                            if k == 'COLUMN_MAX_AND_MIN':
                                row = df.selectExpr("max({}) as max".format(metric.column_name),
                                                    "min({}) as min".format(metric.column_name)).first()
                                v = '[{0},{1}]'.format(row.min, row.max)

                            if k == 'COLUMN_EUM':
                                row = df.agg(collect_set(metric.column_name).alias("value")).first()
                                v = str(row.value).replace("'", '"')

                            if k == 'TABLE_PK_UNIQUE':
                                if not metric.pk_column_name:
                                    logger.info("主键未维护，请维护主键...")
                                    continue
                                row = df.groupby(*(metric.pk_column_name.split(","))).agg(
                                    count("*").alias("cnt")).filter(
                                    "cnt >1").first()
                                if row:
                                    v = "false"
                                else:
                                    v = "true"

                            if k == 'COLUMN_EMPTY_COUNT':
                                row = df.filter("{0} = '' or {0} is null".format(metric.column_name)).agg(
                                    count("*").alias("cnt")).first()
                                v = str(row.cnt)

                            if k == 'RAW_COUNT':
                                v = str(df.count())

                            if k == 'TABLE_DATA_CREATE_TIME':
                                v = TABLE_DATE_CREATE_TIME

                        if k == 'CUSTOM_SQL':
                            row = self.spark.sql(metric.sql).first()
                            v = str(row[0])

                    values[k] = v
                    metric.metric_value = v
                    logger.info(metric.type + ": " + v)
                dqc_end = datetime.now()
                duration = round((dqc_end - dqc_start).seconds / 60.0, 2)
                logger.info("数据质量指标计算结束 : " + dqc_end.strftime("%Y-%m-%d %H:%M:%S"))
                logger.info("数据质量指标计算耗时 : " + str(duration) + "分钟")
        except Exception as e:
            logger.info("数据质量指标计算异常: " + str(e))

    def register_external_tables(self):
        external_tables = eval(self.config.external_tables)
        if not external_tables:
            return
        for ext_type, tables in external_tables.items():
            if ext_type == 'kudu':
                for table in tables:
                    spark_table = table[8:].replace(".", "_")
                    df = self.spark.read \
                        .option("kudu.master", self.config.host) \
                        .option("kudu.table", table) \
                        .format("kudu") \
                        .load()
                    df.createOrReplaceTempView(spark_table)
                    logger.info("成功注册kudu表 : " + table)
            if ext_type == 'phoenix':
                for table in tables:
                    spark_table = table.replace(".", "_")
                    df = self.spark.read \
                        .format("org.apache.phoenix.spark") \
                        .option("table", table) \
                        .option("zkUrl", self.config.host) \
                        .load()
                    df.createOrReplaceTempView(spark_table)
                    logger.info("成功注册phoenix表 : " + table)

    def register_udf(self):
        udf_list = self.config.source.udfs
        if len(udf_list) > 0:
            from pyspark.sql.types import IntegerType, BooleanType, FloatType, DoubleType, StringType, DateType, \
                TimestampType, DecimalType
            mapping = {
                'String': StringType,
                'Boolean': BooleanType,
                'Double': DoubleType,
                'Float': FloatType,
                'Integer': IntegerType,
                'Date': DateType,
                'Timestamp': TimestampType,
                'Decimal': DecimalType
            }
            for udf in udf_list:
                ret_type = StringType()
                if udf.return_type:
                    ret_type = mapping.get(udf.return_type)()
                if udf.udf_type == 'PythonFunction':
                    m = __import__(udf.source_name)
                    f = getattr(m, udf.function_name)
                    self.spark.udf.register(udf.udf_name, f, ret_type)
                if udf.udf_type == 'JavaFunction':
                    self.spark.udf.registerJavaFunction(udf.udf_name, udf.source_name, returnType=ret_type)
                if udf.udf_type == 'JavaUDAF':
                    self.spark.udf.registerJavaUDAF(udf.udf_name, udf.source_name)

    @staticmethod
    def replace_time_vars(sql_text):
        sql_vars = {"bizdate": (datetime.today() + timedelta(-1)).strftime('%Y%m%d'),
                    "bizyear": (datetime.today() + timedelta(-1)).strftime('%Y'),
                    "bizquarter": (datetime.today() + timedelta(-1)).strftime('%Y') + str(
                        int(math.ceil(int((datetime.today() + timedelta(-1)).strftime('%m')) / 3.0))).rjust(2, '0'),
                    "bizmonth": (datetime.today() + timedelta(-1)).strftime('%Y%m')}
        tem = string.Template(sql_text)
        sql = tem.safe_substitute(sql_vars)
        return sql

    def repartition(self, df):
        if self.spark.catalog._jcatalog.tableExists(self.config.table):
            self.spark.sql("ANALYZE TABLE {} COMPUTE STATISTICS NOSCAN".format(self.config.table))
            s = self.spark.sql("DESCRIBE EXTENDED {} ".format(self.config.table)).filter(
                col("col_name") == "Statistics").first()
            if len(s) > 0:
                table_size_str = s.data_type.split(", ")[0]
                if table_size_str == '0 bytes':
                    return df
                table_size = float(table_size_str[0:-6])
                table_size_m = math.ceil(float(table_size / 1024 / 1024))
                num_partitions = math.ceil(
                    table_size / self.spark._jsc.hadoopConfiguration().set("dfs.block.size", "128m"))
                if num_partitions > 0:
                    logger.info("开始重新分区...")
                    logger.info("表大小约为: " + str(table_size_m) + "M")
                    logger.info("分区数为: " + str(num_partitions))
                    df = df.repartition(int(num_partitions))
                else:
                    return df
        else:
            cnt = df.count()
            logger.info("sql结果行数为 : " + str(cnt))
            num_partitions = math.ceil(float(cnt) / self.config.target.count_per_partitions)
            if num_partitions > 1:
                if num_partitions > 1000:
                    num_partitions = 1000
                logger.info("开始重新分区...")
                logger.info("每个分区数据条数为 : " + str(self.config.target.count_per_partitions))
                logger.info('分区数为 : ' + str(num_partitions))

                df = df.repartition(int(num_partitions))

        return df
