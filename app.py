from tweepy import StreamListener, OAuthHandler, Stream
import json
from kafka import KafkaProducer, KafkaConsumer
topic_name = 'timothy_twitter_stream_topic'
producer = KafkaProducer(
    bootstrap_servers='52.19.199.252:9092,52.19.199.252:9093,52.19.199.252:9094',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)




class Authenticator:
    """

    """

    def __init__(self):
        pass

    def auth_twitter_app(self):
        auth = OAuthHandler("BSu45Bjg7VnIpdLh54wUHMuCx", "Uy8wHfdDOqWzYwRbqeVOPCmpjHHvxcGQwtQ7kexInOihqM7qMr")
        auth.set_access_token("1148876239274500096-YVcsYBO3RfWPYqUW17D717voCeTcw1",
                              "vZ7wItwK19qlb4OcbecPhoUPVPpM2aMwY9nik9G9Wp7tb")
        return auth


class Streamer:
    """

    """
    def __init__(self):
        pass

    def stream(self):
        auth = Authenticator().auth_twitter_app()
        stream = Stream(auth, ListenerFunction())
        stream.filter(track=track_list)


class ListenerFunction(StreamListener):
    """

    """

    def on_data(self, data):
        Data = json.loads(data)
        # stringData = json.dumps(Data, indent=2)

        self.dataDict = {
            'text': Data['text'],
            'created_at': Data['created_at'],
            'tweet_id': Data['id'],
            'followers_count': Data['user']['followers_count'],
            'user_id': Data['user']['id'],
            'profile_img_url': Data['user']['profile_image_url'],
            'username': Data['user']['screen_name'],
            'retweeted': Data['retweeted'],
            'retweeet_count': Data['retweet_count'],
            'source': Data['source']
        }

        producer.send(topic_name, value=self.dataDict)

    def on_error(self, status):
        if status == 420:
            return False
        print(status)


if __name__ == "__main__":
    track_list = ['Nigeria', 'Lagos', 'Buhari']
    Streamer().stream()









