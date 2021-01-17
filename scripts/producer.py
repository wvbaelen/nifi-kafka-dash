
from time import sleep
from json import dumps
from random import randint
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))

for j in range(1, 6):
    print("Iteration", j)
    data = {'counter': randint(1,100)}
    producer.send('BTC', value=data)
    sleep(0.5)

for j in range(1, 6):
    print("Iteration", j)
    data = {'counter': randint(1,100)}
    producer.send('ETH', value=data)
    sleep(0.5)

for j in range(1, 6):
    print("Iteration", j)
    data = {'counter': randint(1,100)}
    producer.send('LINK', value=data)
    sleep(0.5)
