
from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'BTC',
    bootstrap_servers=['localhost:9092'],
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

while True:
    messages = consumer.poll(timeout_ms=500)
    for tp, msg in messages.items():
        print(f"{tp.topic}:{tp.partition}:{msg[0].offset}: value={msg[0].value}")

import plotly.graph_objects as go
import numpy as np
x = np.linspace(0, 4, 20)

data = [
    go.Scatter(x=x, y=x)
]
data2 = [
    go.Scatter(x=x, y=x)
]

fig = go.Figure(data=data, layout=dict(height=450, width=550))
fig = go.Figure(data=data2, layout=dict(height=450, width=550))

# call fig
fig.show()
fig.data = []
fig = fig.add_trace(go.Scatter(x=x, y=x))

