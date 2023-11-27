import json
from typing import List, Tuple
import torch
from torchdata import IterDataPipe
from kafka import KafkaConsumer, KafkaProducer





class KafkaDataPipe(IterDataPipe):
    """Iterable DataPipe to load data from Kafka

    This DataPipe is designed to be used with KafkaConsumer.
    
    It reads an infinite stream of data from Kafka and returns a datapoint at a time
    """



    def __init__(self, consumer: KafkaConsumer):
        super().__init__()
        self.consumer = consumer
    
    def __iter__(self):
        while True:
            data = self.consumer.read()
            data = json.loads(data)
            yield data


class KafkaBatcherDataPipe(IterDataPipe):
    """Converts the stream of data from Kafka into batches

    This DataPipe is responsible for requesting data from Group 2 and the batching logic.
    """



    def __init__(self, dp: IterDataPipe, producer: KafkaProducer, batch_size: int = 64):
        super().__init__()
        self.dp = dp
        self.producer = producer
        self.batch_size = batch_size
    
    def __iter__(self):
        batch = []
        self.producer.start()
        self.request_batch()

        for data in self.dp:
            if len(batch) == self.batch_size:
                batch = self.prepare_batch(batch)
                yield batch
                batch = []
                self.request_batch()
            
            batch.append(data)
        
        if len(batch) > 0:
            batch = self.prepare_batch(batch)
            yield batch
        
        self.producer.close()
    
    def request_batch(self):
        """This function is responsible for clearing the kafka topic and requesting a new batch of data from Group 2
        """
        
        # TODO - Clear the Kafka topic

        self.producer.write("BATCH\n")

    
    def prepare_batch(self, batch: List[Tuple[torch.Tensor, torch.Tensor]]) -> Tuple[torch.Tensor, torch.Tensor]:
        """Prepare the batch for training

        This method is responsible for converting the batch into a format that can be used for training.
        """
        
        batch = list(zip(*batch))
        batch = [torch.stack(b) for b in batch]
        return batch


class KafkaDeserializerDataPipe(IterDataPipe):
    """Converts the stream of data from a dictionary to a pytorch tensor with the correct label"""

    def __init__(self, dp: IterDataPipe):
        super().__init__()
        self.dp = dp
    
    def __iter__(self):
        for data in self.dp:
            image = data["image"]
            label = data["label"]

            image = torch.tensor(image)
            label = torch.tensor(label)

            yield image, label