import os
import subprocess


ZOOKEEPER_SERVER_PORT = 2181
KAFKA_SERVER_PORT = 9092
BASE_DIR = "/opt/kafka"

LOGS_DIR = "logs"


class KafkaServer:
    def __init__(self, server_name: str = "localhost"):
        self.server_name = server_name

        self.zookeeper_stdout = os.path.join(LOGS_DIR, "zookeeper.log")
        self.zookeeper_stderr = os.path.join(LOGS_DIR, "zookeeper.err")

        self.kafka_stdout = os.path.join(LOGS_DIR, "kafka.log")
        self.kafka_stderr = os.path.join(LOGS_DIR, "kafka.err")

    def start_zookeeper(self):
        print(f"{os.getpid()}: Starting Zookeeper Server")

        start_script_path = os.path.join(BASE_DIR, "bin/zookeeper-server-start.sh")
        config_path = os.path.join(BASE_DIR, "config/zookeeper.properties")

        # Start the zookeeper server and pipe the outputs to the log files
        with open(self.zookeeper_stdout, "w") as stdout, open(
            self.zookeeper_stderr, "w"
        ) as stderr:
            try:
                subprocess.run(
                    ["bash", start_script_path, config_path],
                    stdout=stdout,
                    stderr=stderr,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Error while starting zookeeper server: {e.stderr}")

    def start_kafka(self):
        print(f"{os.getpid()}: Starting Kafka Server")

        start_script_path = os.path.join(BASE_DIR, "bin/kafka-server-start.sh")
        config_path = os.path.join(BASE_DIR, "config/server.properties")

        # Start the kafka server and pipe the outputs to the log files
        with open(self.kafka_stdout, "w") as stdout, open(
            self.kafka_stderr, "w"
        ) as stderr:
            try:
                subprocess.run(
                    ["bash", start_script_path, config_path],
                    stdout=self.kafka_stdout,
                    stderr=self.kafka_stderr,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Error while starting kafka server: {e.stderr}")

    def stop_zookeeper(self):
        stop_script_path = os.path.join(BASE_DIR, "bin/zookeeper-server-stop.sh")
        try:
            process = subprocess.run(
                ["bash", stop_script_path], capture_output=True, text=True, check=True
            )
            print(f"Zookeeper Server Stopped: {process.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error while stopping zookeeper server: {e.stderr}")

    def stop_kafka(self):
        stop_script_path = os.path.join(BASE_DIR, "bin/kafka-server-stop.sh")
        try:
            process = subprocess.run(
                ["bash", stop_script_path], capture_output=True, text=True, check=True
            )
            print(f"Kafka Server Stopped: {process.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error while stopping kafka server: {e.stderr}")

    def start(self):
        # Start the zookeeper and kafka servers on separate processes

        # Start the zookeeper server
        zookeeper_process = os.fork()
        if zookeeper_process == 0:
            # child process, start the zookeeper server
            self.start_zookeeper()

        elif zookeeper_process > 0:
            # parent process, start the kafka server
            kafka_process = os.fork()
            if kafka_process == 0:
                # child process, start the kafka server
                self.start_kafka()
            elif kafka_process < 0:
                raise Exception("Error while starting kafka server")

        else:
            raise Exception("Error while starting zookeeper server")

        if zookeeper_process > 0 and kafka_process > 0:
            # Parent process

            # Wait for keyboard input to exit
            key = None
            while key != "q":
                key = input("Press q to quit: ", end="")
                print()

            # Stop both servers
            self.stop_kafka()
            self.stop_zookeeper()

            # Kill all the processes created by this program
            os.kill(kafka_process, 9)
            os.kill(zookeeper_process, 9)

            print("Exiting...")


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
        if self.process is None:
            raise Exception("Producer not started")
        self.process.stdin.write(data.encode())

    def close(self):
        if self.process is None:
            raise Exception("Producer not started")
        self.process.stdin.close()
        self.process.wait()

    def create_topic(self, topic_name, partitions=1, replication_factor=1):
        # Create the topic
        try:
            create_script_path = os.path.join(BASE_DIR, "bin/kafka-topics.sh")
            command = [
                "bash",
                create_script_path,
                "--bootstrap-server",
                f"{self.server_name}:{KAFKA_SERVER_PORT}",
                "--create",
                "--topic",
                topic_name,
                "--partitions",
                f"{partitions}",
                "--replication-factor",
                f"{replication_factor}",
            ]
            process = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
            print(process.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error while creating topic {topic_name}: {e.stderr}")

    def delete_topic(self, topic_name):
        try:
            delete_script_path = os.path.join(BASE_DIR, "bin/kafka-topics.sh")
            command = [
                "bash",
                delete_script_path,
                "--bootstrap-server",
                f"{self.server_name}:{KAFKA_SERVER_PORT}",
                "--delete",
                "--topic",
                topic_name,
            ]
            process = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
            print(process.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error while deleting topic {topic_name}: {e.stderr}")


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
