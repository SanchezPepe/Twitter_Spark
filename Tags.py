#extraction helper
def extraction(jsonObj):
    if ('text' in jsonObj):
        return jsonObj['text']
    else:
        return 'N/A'


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: trend-tags.py <hostname> <port>", file=sys.stderr)

    sc = SparkContext(appName = "tweet-trending")
    timeIntervalsecs = 8
    ssc = StreamingContext(sc, timeIntervalsecs)

tweetStream = ssc.socketTextStream('localhost', 4040)

#Start pipeline process
(tweetStream
    .map(lambda tweet: extraction(json.loads(tweet))) #pull the actual tweet
    .flatMap(lambda text: text.split(' ')) #isolating separate words of tweet
    #.foreachRDD(lambda rdd: rdd.toDF().registerTempTable('tweets_from_stream'))
)

ssc.start() #start collection process

#Show the tweetStream
print(tweetStream)

ssc.awaitTermination()