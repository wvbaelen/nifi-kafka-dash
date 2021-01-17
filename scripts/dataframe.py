import pandas as pd
import random

from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'BTC',
    bootstrap_servers=['localhost:9092'],
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

df = pd.DataFrame(columns = ["Speed", "SpeedError", "Direction"])
while True:
    messages = consumer.poll(timeout_ms=500)
    for tp, msg in messages.items():
        print(f"{tp.topic}:{tp.partition}:{msg[0].offset}: value={msg[0].value}")
        row = pd.DataFrame([[msg[0].value, random.uniform(0, 5), 100+random.uniform(-5, 5)]],
            columns = ["Speed", "SpeedError", "Direction"])
        df = df.append(row).tail(100)

df
