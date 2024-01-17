from datetime import datetime
from airflow import DAG
# from airflow.operators.python import PythonOperator
import uuid
import json
import requests
from kafka import KafkaProducer

default_args = {
    'owner': 'Ayman',
    'start_date': datetime(2024, 1, 16, 10, 00)
}


def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    data = get_data()
    data = format_data(data)
    print(data)
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(data).encode('utf-8'))


# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )


stream_data()
