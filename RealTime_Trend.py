import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from credentials import consumer_key, consumer_secret, access_token, access_token_secret


class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data.rstrip()) #removes whitespace from data
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
stream.filter(track=['Celtics', 'BostonCeltics'])