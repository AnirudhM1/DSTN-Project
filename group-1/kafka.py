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
        self.config = {
            "bootstrap.servers": server_name,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
        self.consumer = None
    
    def start(self):
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic_name])

    
    def read(self, timeout: int = 1_000_000):
        if self.consumer is None:
            self.start()
        
        msg = self.consumer.poll(timeout)
        return msg.value().decode()

    def close(self):
        self.consumer.close()
        self.consumer = None


def clear_topic(server_name: str = "localhost:9092"):
    command = os.path.join(BASE_DIR, "bin", "kafka-delete-records.sh")
    subprocess.run([command, "--bootstrap-server", server_name, "--offset-json-file", "/home/anirudh/extra/dstn/project/group-1/delete-records.json"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)