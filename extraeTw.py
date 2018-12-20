"""
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

ckey = 'wAiOgsZu8811j2Ac3mnyquwiT'
csecret = 'kOT4i7K8OoQnNZoDuYEHrsg5DmAW0TnpVyRWPVWWgr4NiYQmm0'
atoken = '26922451-bGQYatrkw4zQZgl5qwIwO8nQXtIln0ZbScSmp1Rqv'
asecret = '1ZvfJFdBNSOBqmDmriOXqURGsO5Yudj4s8597LCqe9Wo5'

class listener(StreamListener):
    def on_data(self,data):
        print(data)
        return True
    def on_error(self,status):
        print(status)
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())

twitterStream.filter(track=["Tren Maya"])

"""

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = 'wAiOgsZu8811j2Ac3mnyquwiT'
consumer_secret = 'kOT4i7K8OoQnNZoDuYEHrsg5DmAW0TnpVyRWPVWWgr4NiYQmm0'
access_token = '26922451-bGQYatrkw4zQZgl5qwIwO8nQXtIln0ZbScSmp1Rqv'
access_secret = '1ZvfJFdBNSOBqmDmriOXqURGsO5Yudj4s8597LCqe9Wo5'

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads(data)
          self.client_socket.send(msg['lang'].encode('utf-8'))
          self.client_socket.send("\n".encode('utf-8'))
          print(msg['lang'].encode('utf-8'))
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

  twitterStream = Stream(auth, TweetsListener(c_socket))
  twitterStream.filter(track=['trump'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "localhost"      # Get local machine name
  port = 4040                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
