
# [START Data_pipeline]
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators import python


# [START Data_pipeline_define_dag]

default_args = {
    'owner': 'Cristiano Silva',
    'start_date': days_ago(0),
    'email': ['crisosilva88@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'twitter_data_pipeline',
    schedule_interval=timedelta(minutes=5),
    default_args = default_args
    ) as dag:
# [END Data_pipeline_define_dag]

    # [START Data_pipeline_operators]
    def get_trends():
        import tweepy

        # Keys for twitter API
        consumer_key = '5D2W2T07IPRS9iHFQZ1MrqegZ'
        consumer_secret = 'iMcforzvlmZPrQbd6zpMhxr5HgQMKUPlvH12ndOEyC2JQlJTVM'
        access_token = '1110170575727706112-QrQBm9EdnmCrj4yE0oGYITaPxXMoYN'
        access_token_secret = '97fP70IbJtAGJgfl1z7kKnaDChLGXHwrsn4Ie4l7PW3Te'

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # instantiating the API
        api = tweepy.API(auth)

        BRAZIL_WOE_ID = 23424768

        brazil_trends = api.get_place_trends(id=BRAZIL_WOE_ID)
        return brazil_trends

    def transform_twitter_data(data) -> None:

        trends_list = []
        for value in data:
            for trend in value['trends']:
                trends_list.append(trend['name'])

        trends_list.insert(0, datetime.now().strftime('%d/%m/%Y %H:%M'))

        return trends_list[0:11]



    def kafka_publish(b_server, kafka_topic, message) -> None:

        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=b_server, #,value_serializer=lambda x: dumps(x).encode('utf-8')
                                 max_block_ms=60000,
                                 api_version=(0, 10, 2)
                                 )
        data = {'twitter_trends': message}
        try:
            print("aqui")
            producer.send(kafka_topic, value=message)
        except:
            print("Erro ao tentar enviar a mensagem")

    extract = python.PythonOperator(task_id='twitter_extract',
                                    python_callable=get_trends())

    transform = python.PythonOperator(task_id='transform_data',
                                      python_callable=transform_twitter_data())

    send = python.PythonOperator(task_id='kafka_send',
                                 python_callable=kafka_publish(transform.python_callable('localhost:9092','tts_data_pipeline',extract.python_callable)))
    # [END Data_pipeline_operators]

# [ START Data_pipeline_relationships]
extract >> transform >> send
# [ END Data_pipeline_relationships]

# [END Data_pipeline]
