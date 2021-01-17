
from kafka import KafkaConsumer
from json import loads
from time import sleep

consumer = KafkaConsumer(
    'BTC',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for event in consumer:
    event_data = event.value
    print(event_data)
    sleep(0.5)
