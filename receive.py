import os
import sys
import json
from tqdm.auto import tqdm

from kafka import KafkaConsumer
import socket

TOPIC_NAME = 'datasets'
SAVE_DIR = 'datasets'
CHUNK_SIZE = 4096

STORAGE_SERVERS = ['localhost', 'localhost', 'localhost', 'localhost']
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = sys.argv[1]


decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)


is_head_node = False

# Create SAVE_DIR if it doesn't exist
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)


# Create a TCP socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Bind the socket to the port
    s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))

    # Listen for incoming connections
    s.listen()

    
    while True:
        # Wait for a connection
        connection, client_address = s.accept()

        # Check if the node is the head node
        if not is_head_node:
            # Now the node is the head node
            is_head_node = True

            # End the connection
            connection.close()

            # Start Head Node duties
            
            # Create a Kafka consumer
            consumer = KafkaConsumer(topic_name=TOPIC_NAME)

            # Start the consumer
            consumer.start()

            # Read data from the Kafka topic untill the chunk is complete or the topic is complete

            for _ in range(CHUNK_SIZE):
                data = consumer.read()

                # Check if the topic is complete
                if data == 'Complete':
                    break

                # Decode the data
                data = decode(data)

                # Get the name of the file without the extension
                file_name = data['name'].split('.')[0]

                # Save the data to disk
                with open(os.path.join(SAVE_DIR, file_name), 'w') as f:
                    json.dump(data, f)
        
            # Close the consumer
            consumer.close()

            # Send the data to the next node

            # Loop through the remaining nodes and check if they are alive
            # The first alive node is the next head node
            # A node is considered alive if it is listening on its port

            next = get_next_server(CURR)
            while next != CURR:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                    try:
                        sender.connect((STORAGE_SERVERS[next], STORAGE_PORTS[next]))
                    except:
                        # Node is not alive
                        next = get_next_server(next)
                        continue

                    # Current node is no longer the head node
                    is_head_node = False
                    break

        
