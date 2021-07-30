from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator 
import numpy as np
import matplotlib.pyplot as plt


def stream (ssc, pwords, nwors, duration):
    kfstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterSteam'], kafkaParams = {"matadata.broker.list": 'localhost:9092'}
    )
    tweets = kfstream.map(lambda x: x[1].encode("ascii", "ignore"))
    
#Cada elemento dos tweets ira conter o texto do tweet
#Pegamos o rastreamento com um tempo de duracao e printamos a cada passo
    words = tweets.flatMap(lambda line: line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))0
    negative = words.map(lambda word: ('Negative ', 1) if word in pwords else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x,y: x+y)
    runingSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    runingSentimentCounts.print()

#O contador mantem a contagem de palavras em todos os intervalos de tempo

    counts = []
    sentimentCounts.forachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
#Start
    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.storp(stopGraceFully = True)
    return counts
    
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def load_wordlist(filename):
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    for line in text:
        words[line]= 1
    f.close()
    return words


    
    
if __name__=="__main__":
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    
    #Cria um Stream context com um intervalo de batch de 10 sec
    ssc = StreamingContext(sec, 10)
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("./Data/positive.txt")
    nwords = load_wordlist("./Data/negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)