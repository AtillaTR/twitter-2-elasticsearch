import json
import tweepy
import sys
import configparser
from kafka import SimpleProducer, KafkaClient

class MyStreamListener(tweepy.StreamListener):


    def on_status(self, status):

        print (status.author.screen_name)
        print (status.text.encode('utf-8'))


        return  True

    def on_timeout(self):
        print  "Tempo esgotado!"
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
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth, listener=MyStreamListener(api))
    myStream.filter(track=consult, languages=['pt'])


    # myStream.filter(follow=['1264329499568738306'])



