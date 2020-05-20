#!/usr/bin/python
#coding: utf-8

from kafka import KafkaProducer
import codecs
import json

def main():
    producer = KafkaProducer(bootstrap_servers="worker2.hengan.shop:9092",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for i in range(1):
        ack = producer.send('foobar2',{"name":"a"+str(i),"age":i+10})
        metadata = ack.get()
        print(metadata.topic)
        print(metadata.partition)
    producer.flush()
    print(producer.partitions_for('foobar2'))
if __name__ == '__main__':
    main()
