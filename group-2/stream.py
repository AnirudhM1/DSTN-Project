import argparse
from ast import Tuple
import os
from typing import Dict, List
import random
import logging
import sys
import json
import socket

from kafka import KafkaProducer, KafkaConsumer

TOPIC_NAME = None
REQ_TOPIC_NAME = None
BATCH_SIZE = None
DATASET_DIR = None

STORAGE_SERVERS = ["localhost", "localhost", "localhost", "localhost"]
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = None  # To be set by the argument parser


# Utility functions
encode = lambda data: json.dumps(data)
decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)

# Metadata
metadata: Dict[str, List[int]] = json.load(open("metadata.json", "r"))
files = list(metadata.keys())


class Streamer:
    """This is a class for the storage server to stream data to Group 1

    A revolving head architecture is being implemented to ensure that the load is distributed evenly among the storage servers.

    This way even if one storage server is down, the system will still be able to function
    """

    def __init__(self, is_head_node: bool):
        """Initializes the streamer.

        This class keeps track of head node status of the current node

        Args:
            is_head_node (bool): If the current node is the head node at start
        """

        self.offset = 0 # The offset used for batch requests
        self.is_head_node = is_head_node

        # Create a Kafka producer to write data to the Kafka topic
        self.producer = KafkaProducer(topic_name=TOPIC_NAME)

        # Create a Kafka consumer to read requests from the Kafka topic
        self.consumer = KafkaConsumer(topic_name=REQ_TOPIC_NAME)

    
    def fetch_batch(self, batch: List[int]) -> List[str]:
        """Function to fetch the batch of data to be prepared for streaming

        Args:
            batch (List[int]): names of the files to be fetched
        """

        prepared_batch = []

        for name in batch:
            # Read the json file
            file_name = str(name).zfill(6) + ".json"
            data = json.load(open(os.path.join(DATASET_DIR, file_name), "r"))

            # Serialize the data
            data = encode(data)

            prepared_batch.append(data)
        
        return prepared_batch
    
    def write_batch(self, batch: List[str]):
        """Function to write a batch of data to the Kafka topic

        Args:
            batch (List[Dict]): Data to be written
        """
        for data in batch:
            self.producer.write(data)
    
    def start(self):
        """Starts a TCP server and listens for incoming connections"""

        if self.is_head_node:
            _ = input("Press enter to start service\n")

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
                    self.head_node_duties()

                # At this point, the current node is not the head node
                # However, by any chance, if the current node is the head node,
                # then this indicates that all other nodes are down
                # The current node will continue to be the head node

                if self.is_head_node:
                    continue

                # The current node is not the head node

                # Wait for a connection
                connection, client_address = s.accept()

                # Wait for the data
                data = connection.recv(1024).decode()

                # If the data is 'ALIVE', then the node is just being checked for liveness
                # Send an acknowledgement and do nothing
                if 'ALIVE' in data:
                    connection.sendall(b"ACK")
                    connection.close()
                    continue

                # If the data is 'TOKEN', then the head node status is being transferred
                # Send an acknowledgement, receive the offset and make the current node the head node
                if 'TOKEN' in data:
                    logging.info("Received Head Node TOKEN")
                    connection.sendall(b"ACK")
                    self.offset = int(connection.recv(1024).decode())
                    self.is_head_node = True
                    connection.close()
                    continue

                # In the other cases, data is a batch of data
                
                if 'PREPARE' not in data:
                    logging.error(f"Invalid data received: {data}")
                    connection.close()
                    continue

                # Receive and deserialize the batch of data
                batch = decode(data[7:])

                logging.info('Starting to prepare the batch of data')

                # Fetch the batch of data
                prepared_batch = self.fetch_batch(batch)

                # Wait for the request from group 1 to write the batch
                response = connection.recv(1024).decode()

                logging.info('Writing the batch of data to the Kafka topic')

                if response != 'WRITE':
                    logging.error(f"Invalid response received: {response}")
                    connection.close()
                    continue

                # Send an acknowledgement
                connection.sendall(b"ACK")
                
                # Write the batch of data to the Kafka topic
                self.write_batch(prepared_batch)

                logging.info('Batch of data written to the Kafka topic\n\n')



    def head_node_duties(self):
        """Function to perfrom head node duties"""

        logging.info("Starting head node duties\n")

        # Randomly sample a batch of data to send
        batch = random.sample(files, BATCH_SIZE)

        # Determine the storage server which contains each item of the batch
        servers = self.get_servers(batch)


        # Prepare the batch of data which current node has to send
        prepared_batch = self.fetch_batch(servers[CURR])

        # Send the batch of data to the storage servers on request
        self.send_batch_data_to_servers(servers)

        # Write it's own batch of data to the Kafka topic
        self.write_batch(prepared_batch)

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

        1. First the current node will instruct each node to prepare the batch of data

        2. The current node will then wait till it receives a request from group 1 for the batch of data
           This request is to be read with the Kafka consumer
        
        3. The current node will instruct each node to start writing the batch of data to the Kafka topic
        
        NOTE: All this is to be done without disconnecting the TCP connection

        Args:
            servers (List[List[int]]): List of storage servers which contain each item of the batch
        """

        senders: List[Tuple[int, socket.socket]] = []

        for i, server in enumerate(servers):
            # If the data is empty, or is the current server, skip the server
            if i == CURR or not server:
                continue

            sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sender.connect((STORAGE_SERVERS[i], STORAGE_PORTS[i]))
            senders.append((i, sender))

        
        logging.info("Instructing the nodes to start preparing the batch of data")

        # Instruct the nodes to prepare the batch of data
        for i, sender in senders:
            sender.sendall(f'PREPARE{encode(servers[i])}'.encode())
        
        # Wait for the request from group 1
        self.wait_for_batch_request()

        logging.info("Instructing the nodes to start writing the batch of data to the Kafka topic")

        # Instruct the nodes to start writing the batch of data to the Kafka topic
        for i, sender in senders:
            sender.sendall(b'WRITE')
        
        # Wait for acknowledgement from the nodes and close the connections
        for i, sender in senders:
            response = sender.recv(1024).decode()
            if response != 'ACK':
                logging.error(f"Invalid response received: {response} from node {STORAGE_SERVERS[i]}:{STORAGE_PORTS[i]}")
            sender.close()


    def wait_for_batch_request(self):
        """This function waits for a request from group 1 for the batch of data"""

        # Read the request at the current offset from the Kafka topic
        request = self.consumer.read(self.offset)

        if request != 'BATCH':
            logging.error(f"Invalid request received: {request}")
            return


    def transfer_head_node_status(self):
        """Function to transfer head node status to the next node"""

        logging.info("Searching for next head node")

        next_server = get_next_server(CURR)
        while next_server != CURR:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                try:
                    sender.connect(
                        (STORAGE_SERVERS[next_server], STORAGE_PORTS[next_server])
                    )
                    sender.sendall(b"TOKEN")

                    # Receive acknowledgement
                    response = sender.recv(1024).decode()
                    if response != "ACK":
                        raise ConnectionRefusedError("Invalid response")

                    logging.info(f"Next node found: {next_server}")

                    # update and send the offset to the next node

                    sender.sendall(str(self.offset + 1).encode())

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




def set_logging():
    # Create the logging directory
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Get root logger
    logger = logging.getLogger()

    # Set the level of the logger
    logger.setLevel(logging.INFO)

    # Create a formatter for the console handler
    console_formatter = logging.Formatter("[%(levelname)s]: %(message)s")

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add the console handler to the logger
    logger.addHandler(console_handler)

    # Create a formatter for the file handler
    file_formatter = logging.Formatter("%(asctime)s: [%(levelname)s]: %(message)s")

    # Create a file handler
    file_handler = logging.FileHandler("logs/stream.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)


def parse_args() -> argparse.Namespace:
    # Add argument parser
    parser = argparse.ArgumentParser(
        description="Stream data from the storage servers to Group 1"
    )

    parser.add_argument("id", type=int, help="The id of the storage server")

    parser.add_argument("--head", action="store_true", help="The node is a head node")

    parser.add_argument(
        "--topic",
        type=str,
        required=False,
        default="stream",
        help="The name of the Kafka topic where streaming data will be written",
    )

    parser.add_argument(
        "--req-topic",
        type=str,
        required=False,
        default="req",
        help="The name of the Kafka topic where requests will be sent",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        required=False,
        default=64,
        help="The size of the batch to be sent",
    )

    parser.add_argument(
        "--dataset-dir",
        type=str,
        required=False,
        default="dataset",
        help="The directory where the dataset is stored",
    )

    # Parse the arguments
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = parse_args()

    # Set the global constants
    CURR = 1
    CURR = args.id
    TOPIC_NAME = args.topic
    REQ_TOPIC_NAME = args.req_topic
    BATCH_SIZE = args.batch_size
    DATASET_DIR = "d2/dataset"
    DATASET_DIR = args.dataset_dir

    # This is done to prevent some issues across operating systems
    STORAGE_SERVERS[CURR] = "localhost"

    # Set the logging
    set_logging()

    # Log the arguments
    logging.info(f"Arguments: {args}")
    logging.info(f"ID: {CURR}")
    logging.info(f"Head Node: {args.head}")
    logging.info(f"Topic Name: {TOPIC_NAME}")
    logging.info(f"Request Topic Name: {REQ_TOPIC_NAME}")
    logging.info(f"Batch Size: {BATCH_SIZE}")
    logging.info(f"Dataset Directory: {DATASET_DIR}\n")


    # Create a streamer object
    streamer = Streamer(args.head)

    # Start the streamer
    streamer.start()

