import os
import logging
import json
import argparse

from kafka import KafkaConsumer
import socket

TOPIC_NAME = None
SAVE_DIR = None
CHUNK_SIZE = None

STORAGE_SERVERS = ["localhost", "localhost", "localhost", "localhost"]
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = None  # To be set by the argument parser


# Utility functions
decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)


class Receiver:
    """This is class for the storage server to receive data from Group 1

    A revolving head architecture is being implemented to ensure that the load is distributed evenly among the storage servers.

    This way even if one storage server is down, the system will still be able to function
    """

    def __init__(self, is_head_node: bool):
        """Initializes the receiver.

        The current offset and head node status of current node is kept track of.

        Args:
            is_head_node (bool): If the current node is the head node at start
        """

        self.offset = 0
        self.is_head_node = is_head_node

        # Create SAVE_DIR if it doesn't exist
        if not os.path.exists(SAVE_DIR):
            os.makedirs(SAVE_DIR)

    def start(self):
        """Starts a TCP server and listens for incoming connections"""

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Bind the socket to the port
            s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))
            logging.info(f"Starting server on port: {STORAGE_PORTS[CURR]}")

            # Listen for incoming connections
            s.listen()
            logging.info("Server is listening...\n")

            while True:
                if self.is_head_node:
                    # Start Head Node duties
                    topic_completed = self.head_node_duties()

                    # If the topic is complete, then the storage server can exit
                    if topic_completed:
                        break

                    # End of Head Node duties

                # At this point, the current node is not the head node
                # By any chance, if the current node is the head node,
                # then this indicates that all other nodes are down
                # The current node will continue to be the head node

                if self.is_head_node:
                    continue

                # The current node is not the head node

                logging.info("Waiting for connection...\n")

                # The storage server will now wait till it is the head node
                # The storage server will know that it is the head node when
                # it receives a connection from the previous head node

                connection, client_address = s.accept()
                logging.info(f"Connection received from: {client_address}")

                # Send the acknowledgement. This is done to ensure that the
                # previous head node knows that the connection has been received
                connection.sendall(b"ACK")

                # Make the storage server the head node
                self.is_head_node = True

                # Receive the offset from the previous head node
                self.offset = int(connection.recv(1024).decode())

                # End the connection
                connection.close()

    def head_node_duties(self) -> bool:
        """This function contains the duties of the head node

        Returns:
            bool: If the topic is complete
        """

        logging.info("Starting Head Node duties...\n")

        # Create a Kafka consumer
        consumer = KafkaConsumer(topic_name=TOPIC_NAME)

        # Start the consumer
        consumer.start(self.offset)

        # Read data from the Kafka topic until the chunk is complete or the topic is complete
        topic_completed = self.read_and_save_data(consumer)

        # Close the consumer
        logging.info("Closing Kafka consumer...")
        consumer.close()

        # Check if the topic is complete
        if topic_completed:
            logging.info("Exiting...")
            return True

        # This part is executed only if the topic is not complete

        # Make the next node the head node
        self.transfer_head_node_status()

        return False

    def read_and_save_data(self, consumer: KafkaConsumer) -> bool:
        """This function reads and saves the data from the Kafka topic

        Args:
            consumer (KafkaConsumer): The Kafka consumer used to read data

        Returns:
            bool: If the topic is complete
        """

        for _ in range(CHUNK_SIZE):
            data = consumer.read()

            # Check if the topic is complete
            if "complete" in data.lower():
                logging.info("Topic Complete\n\n")
                return True

            # Decode the data
            data = decode(data)

            # Get the name of the file with json extension
            file_name = data["name"].split(".")[0] + ".json"

            # Save the data to disk
            with open(os.path.join(SAVE_DIR, file_name), "w") as f:
                json.dump(data, f)

        logging.info("Chunk Complete\n")
        return False

    def transfer_head_node_status(self):
        # Loop through the remaining nodes and check if they are alive
        # The first alive node is the next head node
        # A node is considered alive if it is listening on its port
        # and responds with an awknowledgement when a connection is made

        logging.info("Sending head node status to the next node...\n")

        next_server = get_next_server(CURR)
        while next_server != CURR:
            # Create a Client socket and connect to the next node
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                try:
                    sender.connect(
                        (STORAGE_SERVERS[next_server], STORAGE_PORTS[next_server])
                    )

                    # Receive the acknowledgement
                    response = sender.recv(1024).decode()
                    if response != "ACK":
                        raise Exception("Invalid response received")

                    # Send the offset
                    sender.sendall(f"{self.offset + CHUNK_SIZE}".encode())
                    logging.info(f"Next node found: {next_server}")

                except:
                    # For whatever reason, if the node is not alive, then an error will be raised
                    # The next node will be checked if the current node is not alive
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


def parse_args() -> argparse.Namespace:
    # Add argument parser
    parser = argparse.ArgumentParser(description="Receive data from Group 1")

    parser.add_argument(
        "id", type=int, help="The id of the node. This is used to identify the node"
    )

    parser.add_argument("--head", action="store_true", help="The node is a head node")

    parser.add_argument(
        "--topic",
        type=str,
        required=False,
        default="celeba",
        help="The name of the Kafka topic",
    )

    parser.add_argument(
        "--save-dir",
        type=str,
        required=False,
        default="dataset",
        help="The directory to save the data to",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        required=False,
        default=50,
        help="The size of the chunk to be received",
    )

    # Parse the arguments
    args = parser.parse_args()

    return args


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
    file_handler = logging.FileHandler("logs/receive.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)


if __name__ == "__main__":
    args = parse_args()

    # Set the global constants
    CURR = args.id
    TOPIC_NAME = args.topic
    SAVE_DIR = args.save_dir
    CHUNK_SIZE = args.chunk_size

    # This is done to prevent some issues across operating systems
    STORAGE_SERVERS[CURR] = "localhost"

    # Set logging
    set_logging()

    # Log the arguments
    logging.info("Arguments:")
    logging.info(f"ID: {CURR}")
    logging.info(f"Head Node: {args.head}")
    logging.info(f"Topic Name: {TOPIC_NAME}")
    logging.info(f"Save Directory: {SAVE_DIR}")
    logging.info(f"Chunk Size: {CHUNK_SIZE}\n")

    # Create a Receiver object
    receiver = Receiver(args.head)

    # Start the receiver
    receiver.start()
