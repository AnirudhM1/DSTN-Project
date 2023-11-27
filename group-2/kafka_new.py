from confluent_kafka import Producer, Consumer, TopicPartition


class KafkaProducer:
    def __init__(self, topic_name: str, server_name: str = "localhost"):
        self.topic_name = topic_name
        self.producer = Producer({
            "bootstrap.servers": server_name
        })
    
    def write(self, data: str):
        self.producer.poll(0)
        self.producer.produce(self.topic_name, data.encode("utf-8"))
        self.producer.flush()
    
    def close(self):
        self.producer.close()


class KafkaConsumer:
    def __init__(self, topic_name: str, server_name: str = "localhost"):
        self.topic_name = topic_name
        self.config = {
            "bootstrap.servers": server_name,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    
    def read(self, offset, timeout: int = 1_000_000):
        consumer = Consumer(self.config)
        consumer.assign([TopicPartition(self.topic_name, 0, offset)])
        msg = consumer.poll(timeout)
        consumer.close()
        return msg.value().decode()
