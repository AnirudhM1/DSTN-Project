import os
import subprocess
from confluent_kafka import Producer, Consumer, TopicPartition

BASE_DIR = "/opt/kafka"


class KafkaProducer:
    def __init__(self, topic_name: str, server_name: str = "localhost"):
        self.topic_name = topic_name
        self.server_name = server_name
    
    def start(self):
        self.producer = Producer({
            "bootstrap.servers": self.server_name
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
        config = {
            "bootstrap.servers": server_name,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
        self.consumer = Consumer(config)
    
    def start(self, offset: int = 0):
        self.consumer.assign([TopicPartition(self.topic_name, 0, offset)])

    
    def read(self, timeout: int = 1_000_000):
        msg = self.consumer.poll(timeout)
        return msg.value().decode()

    def close(self):
        self.consumer.close()


def clear_topic(topic_name: str, server_name: str = "localhost:9092"):
    command = os.path.join(BASE_DIR, "bin", "kafka-topics.sh")
    subprocess.run([command, "--delete", "--topic", topic_name, "--bootstrap-server", server_name])
    subprocess.run([command, "--create", "--topic", topic_name, "--bootstrap-server", server_name])