import os
from typing import Dict, List
import random
import sys
import json
import socket

# from kafka_new import KafkaProducer, KafkaConsumer
from confluent_kafka import Producer, Consumer, TopicPartition

TOPIC_NAME = "stream"
SAVE_DIR = "dataset"
BATCH_SIZE = 1

STORAGE_SERVERS = ["localhost", "localhost", "localhost", "localhost"]
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = int(sys.argv[1])


# Utility functions
encode = lambda data: json.dumps(data)
decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)


# Metadata
metadata: Dict[str, List[int]] = json.load(open("metadata.json", "r"))
files = list(metadata.keys())


consumer = KafkaConsumer(topic_name="req")




class Streamer:
    def __init__(self, is_head_node: bool):
        self.is_head_node = is_head_node
        self.producer = KafkaProducer(topic_name=TOPIC_NAME)
        self.producer.start()
        self.offset = 0
    
    def write_batch(self, batch: List[int]):
        for name in batch:
            # Read the json file
            file_name = name + ".json"
            data = json.load(open(os.path.join(SAVE_DIR, file_name), "r"))
            
            # Serialize the data
            data = encode(data)

            # Write the data to the Kafka topic
            self.producer.write(data)
            self.producer.write("\n")
        
    
    def start(self):
        if self.is_head_node:
            _ = input("Press any key to start the fetch process...")
        
        # Start a TCP server

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Bind the socket to the port
            s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))
            print("Starting server on port:", STORAGE_PORTS[CURR])

            # Listen for incoming connections
            s.listen()
            print("Server is listening...")

            while True:
                if self.is_head_node:
                    # Head node duties

                    self.head_node_duties()

                # If the node is not the head node, it is a storage node

                # Wait for a connection
                connection, client_address = s.accept()
                print("Connection received from:", client_address)

                # Wait for the data
                data = connection.recv(BATCH_SIZE * 32).decode()

                # If the data is 'ALIVE', the node is just being checked for liveness
                # Do nothing in this case
                if "ALIVE" in data:
                    connection.sendall(b"ACK")
                    connection.close()
                    continue

                # If the data is 'TOKEN', the node is the next head node
                # Make the node the head node
                if "TOKEN" in data:
                    connection.sendall(b"ACK")
                    self.offset = int(connection.recv(1024).decode())
                    connection.close()
                    self.is_head_node = True
                    continue

                # In all other cases, the data is a batch of data

                # Deserialize the data
                data = decode(data)

                # Write the data to the Kafka topic
                self.write_batch(data)
                connection.close()
    

    def head_node_duties(self):
        # Determine the batch of data to send
        batch = random.sample(files, BATCH_SIZE)

        # Determine the storage server which contains each item of the batch
        servers = self.get_storage_servers(batch)

        # Wait for Batch Request from group 1
        print("Waiting for batch reqest...")
        self.wait_for_batch_request()
        print("Batch request received")

        # Send the batch to the respective storage servers
        self.send_data_to_servers(servers)

        # Write the batch to the Kafka topic
        # for _ in range(2):
        self.write_batch(servers[CURR])

        # Make the next node the head node
        self.transfer_head_node_status()
    

    def wait_for_batch_request(self):
        # consumer.close()
        consumer.start(self.offset)
        req = consumer.read()
        if "BATCH" not in req:
            raise ValueError(f"Invalid request received: {req}")
        
        consumer.close()
    

    def send_data_to_servers(self, servers: List[List[int]]):
        for i, server in enumerate(servers):
            # If the data is empty, or is the current server, skip the server
            if i == CURR or not server:
                continue

            # Send the data to the server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                sender.connect((STORAGE_SERVERS[i], STORAGE_PORTS[i]))
                sender.sendall(encode(server).encode())
    

    def get_storage_servers(self, batch: List[int]) -> List[List[int]]:
        servers = [[] for _ in range(len(STORAGE_SERVERS))]
        for name in batch:
            nodes = metadata[name]

            for node in nodes:
                # Check if the node is alive

                # Current node is always alive
                if node == CURR:
                    break

                # Start a TCP connection with the node
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                    try:
                        sender.connect((STORAGE_SERVERS[node], STORAGE_PORTS[node]))
                        sender.sendall("ALIVE".encode())
                        response = sender.recv(1024).decode()
                        if response != "ACK":
                            raise ConnectionRefusedError("Invalid response received")
                        # Connection successful
                        break
                    except ConnectionRefusedError:
                        print(f"Node {node} is not alive")

            servers[node].append(name)

        return servers
        
    
    def transfer_head_node_status(self):
        # Loop through the remaining nodes and check if they are alive
        # The first alive node is the next head node
        # A node is considered alive if it is listening on its port

        next = get_next_server(CURR)
        while next != CURR:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                try:
                    sender.connect((STORAGE_SERVERS[next], STORAGE_PORTS[next]))
                    sender.sendall("TOKEN".encode())
                    response = sender.recv(1024).decode()
                    if response != "ACK":
                        raise ConnectionRefusedError("Invalid response received")
                    sender.sendall(str(self.offset+1).encode())
                    print("Next node found:", next)

                except:
                    # Node is not alive
                    next = get_next_server(next)
                    continue

                # Current node is no longer the head node
                self.is_head_node = False
                break
    

if __name__ == "__main__":
    is_head_node = CURR == 0
    streamer = Streamer(is_head_node)
    streamer.start()