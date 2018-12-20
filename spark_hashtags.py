import logging

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, HiveContext
from pyspark import (SparkConf, SparkContext, SQLContext, Row)


# set up logging
logging.getLogger().setLevel(
    level=logging.ERROR
)

# Create a local StreamingContext with two working thread
# and batch interval of 1 second
conf = SparkConf().setMaster("local[2]").setAppName("SparkTwitter")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 1)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# Create a DStream that will connect to hostname:port, like localhost:50001
hashtag_DS = ssc.socketTextStream("localhost", 4040) 




def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)

    return globals()['sqlContextSingletonInstance']

from pyspark.sql.types import *
schema = StructType([])
sql_context = HiveContext(sc)
empty = sql_context.createDataFrame(sc.emptyRDD(), schema)


def process_rdd(_, rdd):
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        empty = sql_context.sql(
            "SELECT hashtag, hashtag_count "
            "FROM hashtags "
            "ORDER BY hashtag_count "
            "DESC "
            "LIMIT 20"
        )
        empty.show() 
    except Exception as e:
        logging.error(e)


tags_DS = hashtag_DS.map(lambda word: (word.lower(), 1))\
    .updateStateByKey(aggregate_tags_count)

# do processing for each RDD generated in each interval
tags_DS.foreachRDD(process_rdd)

ssc.start()

import matplotlib.pyplot as plt
import seaborn as sn
import time
from IPython import display

count = 0
while count < 10:
  time.sleep(40)
  top_10_tweets = sql_context.sql(
            "SELECT hashtag, hashtag_count "
            "FROM hashtags "
            "ORDER BY hashtag_count "
            "DESC "
            "LIMIT 20"
        )
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 10, 5 ) )
  sn.barplot( x="hashtag", y="hashtag_count", data=top_10_df)
  plt.xticks(rotation=25)
  plt.show()
  count = count + 1



ssc.awaitTermination()
