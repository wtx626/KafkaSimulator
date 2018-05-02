#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/2 9:38
# @Author  : wutianxiong
# @File    : kafka-consumer.py
# @Software: PyCharm

from constant import hosts
from pykafka import KafkaClient

client = KafkaClient(hosts=hosts)
# 消费者
topic = client.topics[b'kafka_test']
consumer = topic.get_simple_consumer(consumer_group=b'test1', auto_commit_enable=True, auto_commit_interval_ms=1,
                                     consumer_id=b'test1')
for message in consumer:
    if message is not None:
        print(message.offset, message.value)
