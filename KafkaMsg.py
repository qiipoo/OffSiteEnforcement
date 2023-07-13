# -*- coding: utf-8 -*-

import sys
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import numpy as np
import pandas as pd
from datetime import datetime


def on_send_success(record_metadata=None):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp=None):
    print('ErrBack')


def msg_produce(topic, data):

    producer = KafkaProducer(
        bootstrap_servers='edge:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        sasl_mechanism="PLAIN",
        retries=3
    )
    future = producer.send(topic, data)
    # producer.send('sj_gps_offline', data_dict).add_callback(on_send_success).add_errback(on_send_error)
    # producer.flush()
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        print('Send Error')
        return
    topic = record_metadata.topic
    partition = record_metadata.partition
    offset = record_metadata.offset
    print('Send Success, Topic: {}, Partition: {}, Offset: {}'.format(topic, partition, offset))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('input send file please')
        exit(1)
    file_path = sys.argv[1]
    file_name = str.split(file_path, '/')[-1]

    if not (file_name.startswith('sj_gps_offline')
            and file_name.startswith('sj_gps_offline')
            and file_name.startswith('sj_gps_offline')):
        print('file illegality')
        exit(1)
    df = pd.read_csv(file_path)
    df.fillna('', inplace=True)
    json_body = df.to_json(orient='records', force_ascii=False)
    lot = datetime.now().strftime('%Y%m%d%H%M%S')
    json_data = {
        "lot": lot,
        "data": json_body
    }
    print(json_data)
    msg_produce(file_name.split('.')[0], json_data)
    pass
