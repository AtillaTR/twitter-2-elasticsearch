import configparser
import tweepy
from confluent_kafka import Producer    
from confluent_kafka.admin import NewTopic 


import socket

def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

class MyStream(tweepy.Stream):  
    def kafkaProducer(self):
        
        confProducer = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
        self.topic = NewTopic(topic= "tweets", num_partitions=3, replication_factor=1 )
        self.producer = Producer(confProducer)
            
    
    
    def on_status(self, status):
            
            self.producer.produce("tweets",key=status.author.screen_name, value=status.text, callback=acked)
            self.producer.poll(1)

           
            
              
    


if __name__ == '__main__':

    
    credentials = configparser.ConfigParser()
    credentials.read("twitter-credentials.txt")
    consumer_key=credentials["DEFAULT"]["consumerKey"]
    consumer_secret=credentials["DEFAULT"]["consumerSecret"]
    access_key=credentials["DEFAULT"]["accessToken"]
    access_secret=credentials["DEFAULT"]["accessTokenSecret"]
    stream = MyStream(consumer_key, consumer_secret, access_key, access_secret)
    stream.kafkaProducer()
    stream.filter(track=["one piece"])
    

