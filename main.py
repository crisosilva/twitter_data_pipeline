#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# [START Twitter_data_pipeline]
# [START import_modules]
from datetime import datetime, timedelta

from textwrap import dedent

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
# [END import_modules]


# [START defining_args]
default_args = {
    'owner': 'Cristiano Silva',
    'start_date': days_ago(0),
    'email': ['crisosilva88@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#  [END defining_args]

# [START instantiate_DAG]
with DAG(
    'twitter_data_pipeline',
    schedule_interval=timedelta(minutes=5),
    default_args = default_args
    ) as dag:
    # [END instantiate_DAG]

    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract]
    def extract(**kwargs):
        import tweepy
        ti = kwargs['ti']

        # Keys for twitter API
        consumer_key = 'xxx'
        consumer_secret = 'xxx'
        access_token = 'xxx'
        access_token_secret = 'xxx'

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # instantiating the API
        api = tweepy.API(auth)

        BRAZIL_WOE_ID = 23424768

        brazil_trends = api.get_place_trends(id=BRAZIL_WOE_ID)
        ti.xcom_push('data_tts_brazil', brazil_trends)
    # [END extract]

    # [START transform]
    def transform(**kwargs):
        ti = kwargs['ti']

        trends_list = []
        data = ti.xcom_pull(task_ids='extract', key='data_tts_brazil')
        for value in data:
            for trend in value['trends']:
                trends_list.append(trend['name'])

        trends_list.insert(0, datetime.now().strftime('%d/%m/%Y %H:%M'))

        ti.xcom_push('trends', trends_list[0:11])
    # [END transform]

    # [START publish_data]
    def kafka_publish(**kwargs):

        ti = kwargs['ti']
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers='localhost:9090', #,value_serializer=lambda x: dumps(x).encode('utf-8')
                                 max_block_ms=60000,
                                 api_version=(0, 10, 2)
                                 )
        data = {'twitter_trends': ti.xcom_pull(task_ids='transform', key='trends')}
        try:
            print("aqui")
            producer.send('tts_pipeline', value=ti.xcom_pull(task_ids='transform', key='trends'))
        except:
            print("Erro ao tentar enviar a mensagem")
    # [END publish_data]

    # [START main_flow]
    extract_task = PythonOperator(task_id='extract',
                                  python_callable=extract)
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(task_id='transform',
                                    python_callable=transform)
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    publish_task = PythonOperator(task_id='publish',
                                  python_callable=kafka_publish)
    publish_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> publish_task
    # [END main_flow]

# [END Twitter_data_pipeline]
