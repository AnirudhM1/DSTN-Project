import os
from typing import Dict, List
import random
import logging
import sys
import json
import socket

from kafka import KafkaProducer

TOPIC_NAME = "fetch"
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



class Sender:
    """This is a class for the storage server to send data to Group 2

    A revolving head architecture is being implemented to ensure that the load is distributed evenly among the storage servers.

    This way even if one storage server is down, the system will still be able to function
    """
    
    def __init__(self, is_head_node: bool):
        """Initializes the sender.

        This class keeps track of head node status of the current node.

        A Kafka Producer is initialized to write data to the Kafka topic

        Args:
            is_head_node (bool): If the current node is the head node at start
        """

        self.is_head_node = is_head_node

        # Create a Kafka producer
        self.producer = KafkaProducer(topic_name=TOPIC_NAME)

        # Start the producer
        self.producer.start()
    
    def write_batch(self, batch: List[int]):
        """Function to write a batch of data to the Kafka topic

        Args:
            batch (List[int]): names of the files to be written
        """

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
        """Starts a TCP and listens for incoming connections"""

        # Start a TCP server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Bind the socket to the port
            s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))
            logging.info(f"Starting server on port: {STORAGE_PORTS[CURR]}")

            # Listen for incoming connections
            s.listen()
            logging.info("Server is listening...\n")
        
        while True:
            if self.is_head_node:
                # Head node duties
                pass

            # At this point, the current node is not the head node
            # However, by any chance, if the current node is the head node,
            # then this indicates that all other nodes are down
            # The current node will continue to be the head node

            if self.is_head_node:
                continue

            # The current node is not the head node
            
            # TODO: Write the part of a storage server which is not the head node
    
    def head_node_duties(self):
        """Function to perform head node duties"""
        
        # Randomly sample a batch of data to send
        batch = random.sample(files, BATCH_SIZE)

        # Determine the storage server which contains each item of the batch
        servers = self.get_servers(batch)

        # Send the batch of data to the storage servers
        self.send_batch_data_to_servers(servers)

        # Write it's own batch of data to the Kafka topic
        self.write_batch(servers[CURR])

        # Transfer head node status to the next node
        self.transfer_head_node_status()

    
    def get_servers(self, batch: List[int]) -> List[List[int]]:
        """Function to determine which storage server contains each item of the batch

        Args:
            batch (List[int]): names of the files in the batch

        Returns:
            List[List[int]]: List of storage servers which contain each item of the batch
        """

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
                        logging.info(f"Node {node} is not alive")
            servers[node].append(name)
        
        return servers


    def send_batch_data_to_servers(self, servers: List[List[int]]):
        """Function to send the batch information to the respective storage servers

        Args:
            servers (List[List[int]]): List of storage servers which contain each item of the batch
        """
        
        for i, server in enumerate(servers):
            # If the data is empty, or is the current server, skip the server
            if i == CURR or not server:
                continue

            # Send the data to the server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                sender.connect((STORAGE_SERVERS[i], STORAGE_PORTS[i]))
                sender.sendall(b'SENDING BATCH DATA')
                sender.sendall(encode(server).encode())

    def transfer_head_node_status(self):
        """Function to transfer head node status to the next node"""

        next_server = get_next_server(CURR)
        while next_server != CURR:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                try:
                    sender.connect((STORAGE_SERVERS[next_server], STORAGE_PORTS[next_server]))
                    sender.sendall(b'TOKEN')

                    # Receive acknowledgement
                    response = sender.recv(1024).decode()
                    if response != 'ACK':
                        raise ConnectionRefusedError("Invalid response")
                    
                    logging.info(f"Next node found: {next_server}")
                
                except:
                    # Node is not alive
                    next_server = get_next_server(next_server)
                    continue
            
            # The current node is no longer the head node
            self.is_head_node = False
            break
        
        # If the current node is still the head node, then all the nodes are down
        # The current node will continue to be the head node

        if self.is_head_node:
            logging.info("All nodes are down!\n")
            logging.info("Current node continues to be the head node\n")
        else:
            logging.info("Head node status transferred\n")