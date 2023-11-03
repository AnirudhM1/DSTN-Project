import os
import sys
import json
# from tqdm.auto import tqdm

from kafka import KafkaConsumer
import socket

TOPIC_NAME = 'celeba'
SAVE_DIR = 'datasets'
CHUNK_SIZE = 4096

STORAGE_SERVERS = ['localhost', 'localhost', 'localhost', 'localhost']
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = int(sys.argv[1])


decode = lambda data: json.loads(data)
get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)


is_head_node = True if sys.argv[2] == 'true' else False
offset = 0
is_complete = False

# Create SAVE_DIR if it doesn't exist
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)


# Create a TCP socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Bind the socket to the port
    s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))
    print('Starting server on port:', STORAGE_PORTS[CURR])

    # Listen for incoming connections
    s.listen()
    print('Server is listening...')

    
    while True:

        print('\n\n###############################################')
        print('Is head node:', is_head_node)
        print('###############################################\n')

        if is_head_node:
            # Start Head Node duties

            print('Starting Head Node duties...\n')
            
            # Create a Kafka consumer
            consumer = KafkaConsumer(topic_name=TOPIC_NAME)

            # Start the consumer
            consumer.start(offset)

            # Read data from the Kafka topic untill the chunk is complete or the topic is complete

            for _ in range(CHUNK_SIZE):
                data = consumer.read()

                # Check if the topic is complete
                if 'complete' in data.lower():
                    print('Topic Complete')
                    is_complete = True
                    break

                # Decode the data
                data = decode(data)

                # Get the name of the file with json extension
                file_name = data['name'].split('.')[0] + '.json'

                # Save the data to disk
                with open(os.path.join(SAVE_DIR, file_name), 'w') as f:
                    json.dump(data, f)
            
            print('Chunk Complete\nClosing Kafka consumer...')
            
            # Close the consumer
            consumer.close()


            # Check if the topic is complete
            if is_complete:
                print('Topic Complete\n\nExiting...')
                break


            print('Kafka consumer closed\n\nSending head node status to the next node...\n')

            # Send the data to the next node

            # Loop through the remaining nodes and check if they are alive
            # The first alive node is the next head node
            # A node is considered alive if it is listening on its port

            next = get_next_server(CURR)
            while next != CURR:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
                    try:
                        sender.connect((STORAGE_SERVERS[next], STORAGE_PORTS[next]))
                        sender.sendall(f'{offset + CHUNK_SIZE}'.encode())
                        print('Next node found:', next)
                        
                    except:
                        # Node is not alive
                        next = get_next_server(next)
                        continue

                    # Current node is no longer the head node
                    is_head_node = False
                    break


        print('\nWaiting for connection...\n')

        # Wait for a connection
        connection, client_address = s.accept()

        print('Connection received from:', client_address)

        # Check if the node is the head node
        if not is_head_node:
            # Now the node is the head node
            is_head_node = True

            # Receive the offset
            offset = int(connection.recv(1024).decode())

            # End the connection
            connection.close()

            

        
