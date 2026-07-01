from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

producer.produce(
    "uniswap_v3_swaps",
    key="test",
    value="Hello Kafka!"
)

producer.flush()

print("Message sent!")