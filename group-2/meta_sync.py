#!/usr/bin/env python3
import socket
import sys

STORAGE_SERVERS = ["localhost", "localhost", "localhost", "localhost"]
STORAGE_PORTS = [8000, 8001, 8002, 8003]
CURR = int(sys.argv[1])


get_next_server = lambda current_server: (current_server + 1) % len(STORAGE_SERVERS)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Bind the socket to the port
    s.bind((STORAGE_SERVERS[CURR], STORAGE_PORTS[CURR]))
    print("Starting server on port:", STORAGE_PORTS[CURR])

    # Listen for incoming connections
    s.listen()
    print("Server is listening...")

    next = get_next_server(CURR)
    while next != CURR:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sender:
            try:
                sender.connect((STORAGE_SERVERS[next], STORAGE_PORTS[next]))
                sender.sendall(f"".encode())
                print("Next node found:", next)

            except:
                # Node is not alive
                next = get_next_server(next)
                continue
