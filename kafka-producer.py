#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/2 9:34
# @Author  : wutianxiong
# @File    : kafka-producer.py
# @Software: PyCharm

import json
import random
import threading
import time
import uuid

from pykafka import KafkaClient
from constant import hosts

# 创建kafka实例
client = KafkaClient(hosts=hosts)

# 打印一下有哪些topic
print(client.topics)

# 创建kafka producer句柄
topic = client.topics[b'kafka_test']
producer = topic.get_producer()


# work
def work():
    while 1:
        msg = json.dumps({
            "id": str(uuid.uuid4()).replace('-', ''),
            "type": random.randint(1, 5),
            "profit": random.randint(13, 100)})
        producer.produce(bytes(msg, encoding='UTF-8'))


# 多线程执行
thread_list = [threading.Thread(target=work) for i in range(10)]
for thread in thread_list:
    thread.setDaemon(True)
    thread.start()

time.sleep(60)

# 关闭句柄, 退出
producer.stop()
