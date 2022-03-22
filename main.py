#region Libraries

import tweepy  # twitter library
import json  # json library
from json import dumps
from kafka import KafkaProducer

#endregion

#region Twitter API
def api_connect():
    # Keys for twitter API
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # instantiating the API
    api = tweepy.API(auth)

    BRAZIL_WOE_ID = 23424768
    brazil_trends = api.get_place_trends(BRAZIL_WOE_ID)
    trends = json.loads(json.dumps(brazil_trends, indent=1))
    return trends[0]

#endregion

#region Kafka producer
def kafka_config(b_server, kafka_topic, message):
    producer = KafkaProducer(bootstrap_servers=b_server,
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             max_block_ms=60000,
                             api_version=(0, 10, 2)
                             )
    data = {'twitter_trends': message}
    producer.send(kafka_topic, value=message)


topic = 'TG_trends'

#endregion