import json
from logging import exception
import tweepy
import sys
import configparser
from pykafka import  KafkaClient



class MyStreamListener(tweepy.StreamListener):

    def __init__(self,api):
        self.api = api
        super (tweepy.StreamListener,self).__init__()
        client = KafkaClient("localhost:9092")

        self.topic = client.topics['twitterstream']
        
        

    def on_status(self, status):
       
       
        
        menssagem_aux = "Author:"+status.author.screen_name+"\n"+"Tweet:"+status.text
        menssagem_aux =menssagem_aux.encode('utf-8')
        print(menssagem_aux)
        
        with self.topic.get_producer() as producer:
            producer.produce(menssagem_aux)

        return True

            
        
            
        
        

            

        
           

    def on_timeout(self):
        return True

    def on_error(self,status):
        print('Erro do produtor do kafka producer')
        return True


if __name__ == '__main__':


    consult = sys.argv[1:]
    # Requisitando dados para acesso a API
    credentials = configparser.ConfigParser()
    credentials.read("twitter-credentials.txt")
    consumer_key=credentials["DEFAULT"]["consumerKey"]
    consumer_secret=credentials["DEFAULT"]["consumerSecret"]
    access_key=credentials["DEFAULT"]["accessToken"]
    access_secret=credentials["DEFAULT"]["accessTokenSecret"]


    #Criacao do objeto de autentificacao da API
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key,access_secret)
    api = tweepy.API(auth)

    # public_tweets = api.home_timeline()
    # for tweets in public_tweets:
    #     print (tweets.text)



    # api.update_status('tweepy + oauth!')
    #myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth, listener=MyStreamListener(api))
    myStream.filter(track=consult, languages=['pt'])


    # myStream.filter(follow=['1264329499568738306'])



