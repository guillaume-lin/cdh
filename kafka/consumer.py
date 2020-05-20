#!/usr/bin/python
import sys
from kafka import TopicPartition
from kafka import KafkaConsumer
from kafka import OffsetAndMetadata
import json

def main():
    consumer = KafkaConsumer(bootstrap_servers=["worker2.hengan.shop:9092"],group_id='me',
                             auto_offset_reset = 'earliest',
                             #value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=False)
    #consumer.assign([TopicPartition('foobar2',0)])
    consumer.subscribe(['foobar2'])
    #consumer.seek(TopicPartition('foobar2',0),100)
    
    print(consumer.topics())
    print(consumer.subscription())
    ret = consumer.poll()
    print(ret)
    print(consumer.assignment())
    #consumer.seek_to_beginning()
    try:
        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
            print(consumer.partitions_for_topic('foobar2'))
            print("offset is %d" % message.offset)
            tp1 = TopicPartition(topic="foobar2",partition=0)
            om = OffsetAndMetadata(offset=message.offset+1,metadata=1)
            consumer.commit({tp1:om})
            break
    except KeyboardInterrupt:
        sys.exit()    

if __name__ == '__main__':
    main()
