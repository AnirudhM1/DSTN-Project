import os
import json
from tqdm.auto import tqdm

import torchvision.transforms as T
from torchvision.io import read_image

from kafka import KafkaProducer, KafkaServer

DATASET_DIR = 'datasets/img_align_celeba/img_align_celeba'
LABEL_PATH = 'datasets/labels.json'
TOPIC_NAME = 'dataset'




def encode(img_path: str) -> str:
    img = read_image(os.path.join(DATASET_DIR, img_path))
    img = T.Resize((64, 64), antialias=True)(img)

    label = labels[img_path]
    
    data = {
        'name': img_path,
        'label': label,
        'image': img.numpy().tolist() 
    }

    return json.dumps(data)



# Get the list of image names and labels
imgs = os.listdir(DATASET_DIR)
with open(LABEL_PATH, 'r') as f:
    labels = json.load(f)





# Assume that the Kafka server is already running

# Create a Kafka producer
producer = KafkaProducer(topic_name=TOPIC_NAME, server_name='localhost')



# Start producer
producer.start()


# Send the images and labels to the Kafka topic
for img in tqdm(imgs[:10]):
    producer.write(encode(img))
    producer.write('#')

# Close the producer
producer.close()