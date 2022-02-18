from confluent_kafka import Consumer
from elasticsearch import Elasticsearch



running = True
i=0
# Send tweets to bonsai (Elasticsearch Server)
def elasticsearch(message,i):
    try:
        es = Elasticsearch(['https://ou17mxuiuf:yze28xwmh2@twitter-elasticsearc-9497991191.us-east-1.bonsaisearch.net:443'])
        
    

        es.index(index="twitter", ignore=400, doc_type='_doc',id=i, body=message)
        print(f"JSON data {i} has pulled on Elasticsearch")
    except:
        print('Error to send data to Elasticsearch')
    
    
# Loop for kafka data consume
def basic_consume_loop(consumer, topics,i):
    try:
        consumer.subscribe(topics)
        
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
                

                message={
                    "Author": msg.key(),
                    "Tweet": msg.value()
                
                }
                
                
                elasticsearch(message,msg.offset())
                i+=1 
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False


if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "foo",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)
    topics = ["tweets"] 
    basic_consume_loop(consumer,topics,i)