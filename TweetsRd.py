import tweepy
import pprint
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
import time

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
            if "http" not in palabra:
                output.append(palabra)
    pal = ' '.join(output)
    pal = pal.replace('á', 'a')
    pal = pal.replace('é', 'e')
    pal = pal.replace('í', 'i')
    pal = pal.replace('ó', 'o')
    pal = pal.replace('ú', 'u')
    pal = pal.replace('ñ', 'n')
    pal = pal.replace('ñ', 'n')
    pal = pal.replace('#', '')
    pal = pal.replace('Á', 'A')
    pal = pal.replace('É', 'E')
    pal = pal.replace('Í', 'I')
    pal = pal.replace('Ó', 'O')
    pal = pal.replace('Ú', 'U')
    pal = pal.replace('Ñ', 'n')
    pal = pal.replace('#', '')
    return pal

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        i = 0
        try:
            msg = json.loads(data)
            tweet = clean(msg['text'].split())
            pp = pprint.PrettyPrinter(indent=4)
            print("=====================================")
            print("TWEET1: ", tweet.encode())
            print("TWEET2: ",msg['text'].encode())
            print("=====================================")
            try:
                for j in range(10):
                    self.client_socket.send(bytes("{}\n".format(i), "utf-8"))
                    i += 1
                    time.sleep(1)
                self.client_socket.send(tweet.encode())
            except socket.error: pass
            #sent = self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("DATA ERROR: %s \n" % str(e))
        return True 

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])

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
