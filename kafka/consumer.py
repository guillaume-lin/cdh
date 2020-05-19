#!/usr/bin/python
import sys
from kafka import TopicPartition
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(bootstrap_servers=["worker2.hengan.shop:9092"],group_id='me',auto_offset_reset = 'earliest',enable_auto_commit=False)
    #consumer.assign([TopicPartition('foobar2',0)])
    consumer.subscribe(['foobar2'])
    #consumer.seek(TopicPartition('foobar2',0),100)
    #print(consumer.assignment())
    print(consumer.topics())
    print(consumer.subscription())
    
    try:
        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
            print(consumer.partitions_for_topic('foobar2'))
            consumer.commit()
    except KeyboardInterrupt:
        sys.exit()    

if __name__ == '__main__':
    main()
