import os
from typing import Dict, List
import random
import sys
import json
import socket

from kafka import KafkaProducer

TOPIC_NAME = "stream"
SAVE_DIR = "dataset"
BATCH_SIZE = 1

STORAGE_SERVERS = ["localhost", "localhost", "localhost", "localhost"]
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = int(sys.argv[1])


encode = lambda data: json.dumps(data)
decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)

is_head_node = True if sys.argv[2] == "true" else False


# Metadata
metadata: Dict[str, List[int]] = json.load(open("metadata.json", "r"))
files = list(metadata.keys())


# Create a Kafka producer
producer = KafkaProducer(topic_name=TOPIC_NAME)

# Start producer
producer.start()


# Function to write a batch of data to the Kafka topic
def write_batch(batch: List[int]):
    print(batch)
    for name in batch:
        # Read the json file
        file_name = name + ".json"
        with open(os.path.join(SAVE_DIR, file_name), "r") as f:
            data = json.load(f)

        # Serialize the data
        data = encode(data)

        # Write the data to the Kafka topic
        producer.write(data)
        producer.write("\n")


# Start Command
if is_head_node:
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
        if is_head_node:
            # Head node duties

            # Determine the batch of data to send
            batch = random.sample(files, BATCH_SIZE)

            # Determine the storage server which contains each item of the batch
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
                            # Connection successful
                            break
                        except ConnectionRefusedError:
                            print(f"Node {node} is not alive")

                servers[node].append(name)

            # Send the batch to the respective storage servers
            for i, server in enumerate(servers):
                # If the data is empty, or is the current server, skip the server
                if i == CURR or not server:
                    continue

                # Send the data to the server
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                    sender.connect((STORAGE_SERVERS[i], STORAGE_PORTS[i]))
                    sender.sendall(encode(server).encode())

            # Write the batch to the Kafka topic
            write_batch(servers[CURR])

            # Make the next node the head node

            # Loop through the remaining nodes and check if they are alive
            # The first alive node is the next head node
            # A node is considered alive if it is listening on its port

            next = get_next_server(CURR)
            while next != CURR:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                    try:
                        sender.connect((STORAGE_SERVERS[next], STORAGE_PORTS[next]))
                        sender.sendall("TOKEN".encode())
                        print("Next node found:", next)

                    except:
                        # Node is not alive
                        next = get_next_server(next)
                        continue

                    # Current node is no longer the head node
                    is_head_node = False
                    break

        # If the node is not the head node, it is a storage node
        else:
            # Wait for a connection
            connection, client_address = s.accept()
            print("Connection received from:", client_address)

            # Wait for the data
            data = connection.recv(BATCH_SIZE * 32).decode()

            # If the data is 'ALIVE', the node is just being checked for liveness
            # Do nothing in this case
            if "ALIVE" in data:
                connection.close()
                continue

            # If the data is 'TOKEN', the node is the next head node
            # Make the node the head node
            if "TOKEN" in data:
                is_head_node = True
                connection.close()
                continue

            # In all other cases, the data is a batch of data

            # Deserialize the data
            data = decode(data)

            # Write the data to the Kafka topic
            write_batch(data)
            connection.close()
