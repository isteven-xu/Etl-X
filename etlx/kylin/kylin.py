# coding=utf-8
"""
@author: 长风
@contact: xugang_it@126.com
@license: Apache Licence
@file: kylin.py
@time: 2021/4/29 5:05 下午
"""

import requests
import json
import time
from loguru import logger


def run(job):
    cube_name, build_type, start_time, end_time = job.cube_name, job.build_type, job.start_time, job.end_time
    headers = {
        "Authorization": job.env.KYLIN_AUTH,
        "Content-Type": "application/json"
    }
    data = {
        "buildType": build_type
    }

    _URL = job.env.KYLIN_API_URL + "/cubes/" + cube_name + '/build'
    logger.info("kylin host: " + job.env.KYLIN_API_URL)
    logger.info("cube名称: " + cube_name)
    logger.info("构建类型: " + build_type)
    logger.info("构建URL: " + _URL)
    res = requests.put(url=_URL, headers=headers, data=json.dumps(data))
    status = res.status_code
    if status == 200:
        logger.info("cube构建已提交...")
        job_id = res.json().get("uuid")
        _URL = job.env.KYLIN_API_URL + "/jobs/" + job_id
        while True:
            job_res = requests.get(url=_URL, headers=headers).json()
            job_status = job_res.get("job_status")
            if job_status == 'ERROR':
                logger.info("cube构建异常: " + str(job_res))
                break
            if job_status == 'FINISHED':
                logger.info("cube构建结束...")
                break
            logger.info("cube构建中...")
            time.sleep(60 * 5)

    else:
        logger.info(res.content)
        raise RuntimeError("kylin任务提交报错...")
