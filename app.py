import json
import tweepy
import sys
import configparser
from kafka import SimpleProducer, KafkaClient



class MyStreamListener(tweepy.StreamListener):

    def __init__(self,api):
        self.api = api
        super (tweepy.StreamListener,self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client,async = True,batch_send_every_n = 1000,batch_send_every_t=10)

    def on_status(self, status):
       
        print (status.author.screen_name)
        print (status.text.encode('utf-8'))
        menssagem_aux = status.text.encode('utf-8')
        try:
            self.producer.send_messages(b'twitterStream',menssagem_aux)
        except exception as e :
            print(e)
            return False 
        return True    

    def on_timeout(self):
        return True

    def on_error(self,status_code):
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



