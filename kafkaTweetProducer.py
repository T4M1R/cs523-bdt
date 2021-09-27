import sys
import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

class TweetsListener(StreamListener):

    def __init__(self, kafkaProducer):
        super(TweetsListener, self).__init__()
        print ("Tweets Kafka producer initialized")
        self.client_socket = kafkaProducer

    def on_data(self, data):
        try:
            json_data = json.loads(data)
            tweet = json_data["text"]
            print(tweet + "\n")
            self.producer.produce(bytes(json.dumps(tweet), "ascii"))
        
        except BaseException as e:
            print ("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):
        print(status)
        return True


def connect_to_twitter(kafkaProducer, tracks):
    api_key = "Cy25ruxd2DxOHByHwvvvZ34vF"
    api_secret = "gYxJ6gJYAcxvdEg4i4qktsW9SrWwbjcp9GCwlndYEigRJ9SECn"

    access_token = "1262408366095302657-hEPRxRY8KAhqYVq9cuAcTxbSkQmcsd"
    access_token_secret = "7pX54rRFqsILRg5OXKUV5836rRRr7PXE0XLWQ8PTHngfW"
    
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(kafkaProducer))
    twitter_stream.filter(track=tracks, languages=["en"])

if __name__ == "__main__":
    if len(sys.argv) < 5:        
        print ("Usage: spark-submit kafka_hashtag_producer.py <host> <port> <topic_name> <tracks>",
                    file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]
    tracks = sys.argv[4:]

    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    kafkaProducer = kafkaClient.topics[bytes(topic, "utf-8")].get_producer()

    connect_to_twitter(kafkaProducer, tracks)