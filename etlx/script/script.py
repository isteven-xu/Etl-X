# coding=utf-8
"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: script.py
@time: 2021/4/29 5:05 下午
"""
import os
from loguru import logger


def run(job):
    file_name = job.env.HDFS_NFS_PREFIX + job.script_path
    args = job.script_args
    cmd = ''

    if file_name != '' and file_name.endswith(".py"):
        cmd = job.env.ANACONDA_INSTALL_PATH + " " + file_name
    if file_name != '' and file_name.endswith(".sh"):
        cmd = 'sh ' + file_name

    cmd += (" " + args) if args else ""
    cmd = " ".join(cmd.split())

    logger.info("开始运行: " + cmd)
    ret = os.system(cmd)
    if ret != 0:
        raise RuntimeError("命令执行失败！")
