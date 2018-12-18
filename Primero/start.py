# From within pyspark or send to spark-submit:

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 5) # 5 second batch interval

IP = 'localhost'	# Replace with your stream IP
Port = 4040			# Replace with your stream port

lines = ssc.socketTextStream(IP, Port)
lines.pprint()         # Print tweets we find to the console

ssc.start()			   # Start reading the stream
ssc.awaitTermination() # Wait for the process to terminate