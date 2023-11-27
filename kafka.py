import os
import subprocess


ZOOKEEPER_SERVER_PORT = 2181
KAFKA_SERVER_PORT = 9092
BASE_DIR = "/opt/kafka"

LOGS_DIR = "logs"

class KafkaProducer:
    def __init__(self, topic_name: str, server_name: str = "localhost"):
        self.topic_name = topic_name
        self.server_name = server_name
        self.process = None

    def start(self):
        produce_script_path = os.path.join(BASE_DIR, "bin/kafka-console-producer.sh")
        command = [
            "bash",
            produce_script_path,
            "--bootstrap-server",
            f"{self.server_name}:{KAFKA_SERVER_PORT}",
            "--topic",
            self.topic_name,
        ]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def write(self, data: str):
        self.start()
        self.process.stdin.write(data.encode())
        self.close()

    def close(self):
        if self.process is None:
            raise Exception("Producer not started")
        self.process.stdin.close()
        self.process.wait()



class KafkaConsumer:
    def __init__(self, topic_name: str, server_name: str = "localhost"):
        self.topic_name = topic_name
        self.server_name = server_name
        self.process = None

    def start(self, offset: int):
        consumer_script_path = os.path.join(BASE_DIR, "bin/kafka-console-consumer.sh")
        command = [
            "bash",
            consumer_script_path,
            "--bootstrap-server",
            f"{self.server_name}:{KAFKA_SERVER_PORT}",
            "--topic",
            self.topic_name,
            "--partition",
            "0",
            "--offset",
            str(offset),
        ]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def read(self):
        if self.process is None:
            raise Exception("Consumer not started")

        # Read the output
        output = self.process.stdout.readline()
        return output.decode()

    def close(self):
        if self.process is None:
            raise Exception("Consumer not started")
        self.process.stdin.close()
        self.process.kill()
