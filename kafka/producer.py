#!/usr/bin/python
#coding: utf-8

from kafka import KafkaProducer
import codecs

def main():
    producer = KafkaProducer(bootstrap_servers="worker2.hengan.shop:9092")
    for i in range(1000):
        ack = producer.send('foobar2',codecs.encode('my mesage'+str(i)))
        metadata = ack.get()
        print(metadata.topic)
        print(metadata.partition)
    producer.flush()
    print(producer.partitions_for('foobar2'))
if __name__ == '__main__':
    main()
