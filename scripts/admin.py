
from kafka.admin import KafkaAdminClient, NewTopic 

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic_list = []
topic_list.append(NewTopic(name="BTC", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="ETH", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="LINK", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
