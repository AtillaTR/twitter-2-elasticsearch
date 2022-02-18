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
    # Kafka producer writes tweets on topic
    def kafkaProducer(self):

        confProducer = {'bootstrap.servers': "localhost:9092",
                        'client.id': socket.gethostname()}
        self.topic = NewTopic(
            topic="tweets", num_partitions=3, replication_factor=1)
        self.producer = Producer(confProducer)

    def on_status(self, status):

        self.producer.produce(
            "tweets", key=status.author.screen_name, value=status.text, callback=acked)
        self.producer.poll(1)
        print(status.text)

# Take twitter credentials from .txt file
def twitter_credentials():
    try:
        credentials = configparser.ConfigParser()
        credentials.read("twitter-credentials.txt")
        keys = []
        keys.append(credentials["DEFAULT"]["consumerKey"])
        keys.append(credentials["DEFAULT"]["consumerSecret"])
        keys.append(credentials["DEFAULT"]["accessToken"])
        keys.append(credentials["DEFAULT"]["accessTokenSecret"])
        return keys
    
    except:
        print("Error with access keys, please check it")


if __name__ == '__main__':

    keys=twitter_credentials()
    stream = MyStream(keys[0], keys[1], keys[2], keys[3])
    stream.kafkaProducer()
    # Choose a theme for search
    stream.filter(track=["Lost Ark"], languages=['en'])
