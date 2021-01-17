import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import random
import threading
import time
from dash.dependencies import Output, Input
from collections import deque
from kafka import KafkaConsumer
from json import loads


app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    [
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1*1000
        ),
    ]
)


@app.callback(Output('live-graph', 'figure'),
              Input('graph-update', 'n_intervals'))
def update_graph_scatter(input_data):
    data = plotly.graph_objs.Scatter(
            x=list(X),
            y=list(Y),
            name='Scatter',
            mode= 'lines+markers'
            )

    y_min = min(Y)
    ymax = max(Y)

    return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[min(X), max(X)]),
                                                yaxis=dict(range=[y_min-10, ymax+10]),)}


# start streaming messages from kafka topic 'BTC'
X = deque(maxlen=10)
Y = deque(maxlen=10)
[X.append(idx) for idx in [0, 1]]
[Y.append(price) for price in [34000, 36000]]

def poll_kafka():
    consumer = KafkaConsumer(
        'BTC',
        bootstrap_servers=['localhost:9092'],
        group_id=None,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    while True:
        messages = consumer.poll(timeout_ms=500)
        for tp, msg in messages.items():
            print(f"{tp.topic}:{tp.partition}:{msg[0].offset}: value={msg[0].value}")
            Y.append(msg[0].value)
            X.append(X[-1]+1)

stream = threading.Thread(target=poll_kafka, args=())
stream.start()


if __name__ == '__main__':
    app.run_server(debug=True)
