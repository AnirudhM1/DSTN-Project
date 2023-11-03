import json
import os
import sys

from kafka import KafkaProducer, KafkaConsumer

NODE_ID = int(sys.argv[1])
NODE_DIR = f"node{NODE_ID}"
REQ_TOPIC = "batch-req"
RES_TOPIC = "batch-res"
SAVE_DIR = f"{NODE_DIR}/datasets"

offset = 0

with open(f"{NODE_DIR}/meta.json", "r") as JSON:
    json_dict = json.load(JSON)

from collections import defaultdict

image_dict = defaultdict(list)
for dic in json_dict:
    image_dict[dic["img"]].append(dic["node"])

print(image_dict)
consumer = KafkaConsumer(
    topic_name=REQ_TOPIC
)  # all nodes will receive request for the image batch
producer = KafkaProducer(
    topic_name=RES_TOPIC, server_name="localhost"
)  # all nodes can send the images if present


# Start producer and consumer
producer.start()
consumer.start(offset)


while True:
    try:
        request = consumer.read()
        start, end = list(map(int, request.split(";")))
        print(f"{start=}, {end=}")
        for i in range(start, end):
            img_name = f"{i:06}"
            # print(img_name, image_dict[img_name])
            if NODE_ID in image_dict[img_name]:
                print(img_name, image_dict[img_name])
                if image_dict[img_name][0] == NODE_ID:
                    with open(f"{SAVE_DIR}/{img_name}.json", "r") as img:
                        producer.write(img.read())
                    producer.write("#")
                    pass
                image_dict[img_name] = image_dict[img_name][::-1]
        break
    except:
        pass


# Close the producer
producer.close()
