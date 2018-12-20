import logging

import requests

from pyspark import (
    SparkConf,
    SparkContext,
    SQLContext,
    Row
)

from pyspark.streaming import StreamingContext


# Create a local StreamingContext with two working thread
# and batch interval of 1 second
conf = SparkConf().setMaster("local[2]").setAppName("SparkTwitter")
sc = SparkContext(conf=conf)

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


def send_df_to_dashboard(data_frame):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in data_frame.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in data_frame.select("hashtag_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:4045/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


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
        hashtag_counts_df = sql_context.sql(
            "SELECT hashtag, hashtag_count "
            "FROM hashtags "
            "ORDER BY hashtag_count "
            "DESC "
            "LIMIT 20"
        )

        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(hashtag_counts_df)
    except Exception as e:
        logging.error(e)


tags_DS = hashtag_DS.map(lambda word: (word.lower(), 1))\
    .updateStateByKey(aggregate_tags_count)

# do processing for each RDD generated in each interval
tags_DS.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
