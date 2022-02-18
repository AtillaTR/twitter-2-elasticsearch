from __future__ import division
from dataclasses import fields
from confluent_kafka import Consumer
import math
from itertools import islice
# from pykafka import KafkaClient
# from pykafka.common import OffsetType
from afinn import Afinn
import csv
from pykafka import KafkaClient
from pykafka.common import OffsetType
running = True
news_df= []
rows= []
afn = Afinn()  
    
def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print('consume loop')

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                consumer.commit(asynchronous=False)
                # sentiment(msg.value().decode('utf-8'))
                rows=[]
                rows.append([msg.key().decode('utf-8'),msg.value().decode('utf-8')])
                print(rows)
                to_csv()
                
                
                
                

    finally:
        # Close down consumer to commit final offsets.
        
        consumer.close()
def consumer():
    client = KafkaClient()
    topic = client.topics["tweets"]
    consumer = topic.get_simple_consumer(
        auto_offset_reset=OffsetType.LATEST,
        reset_offset_on_start=True)
    LAST_N_MESSAGES = len(consumer._partitions)
    # how many messages should we get from the end of each partition?
    MAX_PARTITION_REWIND = int(math.ceil(LAST_N_MESSAGES / len(consumer._partitions)))
    # find the beginning of the range we care about for each partition
    offsets = [(p, op.last_offset_consumed - MAX_PARTITION_REWIND)
            for p, op in consumer._partitions.items()]
    # if we want to rewind before the beginning of the partition, limit to beginning
    offsets = [(p, (o if o > -1 else -2)) for p, o in offsets]
    # reset the consumer's offsets
    consumer.reset_offsets(offsets)
    for message in islice(consumer, LAST_N_MESSAGES):
        #  news_df.append(message.value.decode('utf-8'))
         rows.append([message.partition_key.decode('utf-8'),message.value.decode('utf-8')])
def sentiment():
    # scores = [afn.score(article)]
    scores = [afn.score(article) for article in news_df]
    score = sum(scores)
    sentiment = ['positive' if score > 0 
                            else 'negative' if score < 0 
                                else 'neutral']
    print(sentiment)

def to_csv():
    
    fields=['Author', 'Tweet']
    filename = "tweets.csv"
    with open(filename, 'a') as csvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile) 
            
        # writing the fields 
        csvwriter.writerow(fields) 
            
        # writing the data rows 
        csvwriter.writerows(rows)

if __name__ == '__main__':
    # consumer stream
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "foo",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}
    
    consumer = Consumer(conf)
    topics = ["tweets"] 
    consume_loop(consumer,topics)

    # consumer per menssages
    # consumer()
    # to_csv()
    # sentiment()
    
    
    
    
                                
