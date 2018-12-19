import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from stop_words import get_stop_words
from nltk.corpus import stopwords

consumer_key = 'jZJhWfSNiBbMp1a6Ov9LrMk92'
consumer_secret = 'CvfvGaoWCwm3tmnusyNlH7JyaPJxeHV918tEYoIm4YASUQxvMP'
access_token = '801576292608921600-3P8OkCsVGUP7gRgYaKmXHGwoERTuZ7q'
access_secret = 'LAABWy0FkvFF4oTF00vn847HZHYh9yYG90qB3OAof0PFo'

stop_words = list(get_stop_words('es'))         #Have around 900 stopwords
nltk_words = list(stopwords.words('spanish'))   #Have around 150 stopwords
stop_words.extend(nltk_words)

def clean(tweet):
    output = []
    for palabra in tweet:
        if not palabra in stop_words:
            output.append(palabra)
    return ' '.join(output)

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          lista =  msg['text'].split()
          tweet = clean(lista) 
          print(tweet)
          self.client_socket.send(tweet.encode())
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['AMLO'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "localhost"      # Get local machine name
  port = 4040                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData(c)
