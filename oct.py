from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, HiveContext
from pyspark import (SparkConf, SparkContext, SQLContext, Row)

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):

    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)

    return globals()['sqlContextSingletonInstance']

conf = SparkConf().setMaster("local[2]").setAppName("SparkTwitter")
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# Create a DStream that will connect to hostname:port, like localhost:50001
hashtag_DS = ssc.socketTextStream("localhost", 4040)

from pyspark.sql.types import *
schema = StructType([])
sql_context = HiveContext(sc)
empty = sql_context.createDataFrame(sc.emptyRDD(), schema)

def process_rdd(_, rdd):
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(idioma=w[0], count=w[1]))
        # create a DF from the Row RDD
        lags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        lags_df.registerTempTable("idiomas")
        # get the top 10 hashtags from the table using SQL and print them
        empty = sql_context.sql(
            "SELECT idioma, count "
            "FROM idiomas "
            "ORDER BY count DESC")
        empty.show() 
    except Exception as e:
        print(e)

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
            "SELECT idioma, count "
            "FROM idiomas "
            "ORDER BY count DESC")
  top_10_df = top_10_tweets.toPandas()
  display.clear_output(wait=True)
  plt.figure( figsize = ( 5, 5 ) )
  sn.barplot( x="idioma", y="count", data=top_10_df)
  plt.show()
  count = count + 1

ssc.awaitTermination()
