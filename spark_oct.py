from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession

sc = SparkContext("local[2]", "Twitter_Streaming")
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)
ssc.checkpoint( "file:///Users/Pepe/Desktop/Github/FinalBDNR/Files/checkpoint")

socket_stream = ssc.socketTextStream("localhost", 4040)
lines = socket_stream.window(10)



# Convert RDDs of the words DStream to DataFrame and run SQL query

def process(time, rdd):
  print("========= %s =========" % str(time))
  print(rdd.collect())
  try:
    # Convert RDD[String] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda w: Row(word=w))
    wordsDataFrame = rdd.toDF(rowRdd)
    # Creates a temporary view using the DataFrame.
    wordsDataFrame.createOrReplaceTempView("words")
    #wordsDataFrame.show(
    # Do word count on table using SQL and print it
    wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word order by count(*) desc")
    wordCountsDataFrame.show()
  except:
    pass

words = lines.map(lambda line: line.split(" "))
words.foreachRDD(process)

ssc.start()



import matplotlib.pyplot as plt
import seaborn as sn
import time
from IPython import display

count = 0
while count < 10:
  time.sleep(60)
  top_10_tweets = sqlContext.sql( 'select word, count(*) as total from words group by word order by count(*) desc' )
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 10, 8 ) )
  sn.barplot( x="count", y="tag", data=top_10_df)
  plt.show()
  count = count + 1


ssc.awaitTermination()

