import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
 
# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    if json_tweet.has_key('lang'): # When the lang key was not present it caused issues
        if json_tweet['lang'] == 'en':
            return True # filter() requires a Boolean value
    return False
 
# SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "TwitterDemo")
ssc = StreamingContext(sc, 10) #10 is the batch interval in seconds
IP = "localhost"
Port = 4040
lines = ssc.socketTextStream(IP, Port)
 
lines.foreachRDD( lambda rdd: rdd.filter( filter_tweets ).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()) )
 
# You must start the Spark StreamingContext, and await process termination…
ssc.start()
ssc.awaitTermination()