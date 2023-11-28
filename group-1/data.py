import json
import threading
from collections import deque
from typing import List, Tuple
import torch
from torchdata.datapipes.iter import IterDataPipe
from kafka import KafkaConsumer, KafkaProducer, clear_topic


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

class KafkaBatcherDataPipe(IterDataPipe):
    """Converts the stream of data from Kafka into batches

    This DataPipe is responsible for requesting data from Group 2 and the batching logic.
    """

    def __init__(self, dp: IterDataPipe, consumer: KafkaConsumer, producer: KafkaProducer, batch_size: int = 64):
        super().__init__()
        self.dp = dp
        self.producer = producer
        self.consumer = consumer
        self.batch_size = batch_size

    def __iter__(self):
        batch = []
        self.producer.start()
        self.consumer.start()
        self.request_batch()

        for data in self.dp:
            batch.append(data)
            if len(batch) == self.batch_size:
                batch = self.prepare_batch(batch)
                yield batch
                batch = []
                self.request_batch()


        if len(batch) > 0:
            batch = self.prepare_batch(batch)
            yield batch

        self.producer.close()

    def request_batch(self):
        """This function is responsible for clearing the kafka topic and requesting a new batch of data from Group 2"""

        clear_topic()
        self.producer.write("BATCH")

    def prepare_batch(
        self, batch: List[Tuple[torch.Tensor, torch.Tensor]]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Prepare the batch for training

        This method is responsible for converting the batch into a format that can be used for training.
        """

        batch = list(zip(*batch))
        batch = [torch.stack(b) for b in batch]
        return batch



class PrefetchDataLoader:
    def __init__(self, dataloader, prefetch_factor=2):
        self.dataloader = dataloader
        self.prefetch_factor = prefetch_factor
        self.buffer = deque(maxlen=prefetch_factor)

        self.empty_sem = threading.Semaphore(prefetch_factor)
        self.full_sem = threading.Semaphore(0)
        self.buffer_lock = threading.Lock()

        self.exit_event = threading.Event()
        self.worker_thread = threading.Thread(target=self.fetch, args=(self.exit_event,))
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def fetch(self, exit_event: threading.Event):
        while not exit_event.is_set():
            data = next(iter(self.dataloader))

            self.empty_sem.acquire() # Increment the empty slots in the buffer

            with self.buffer_lock:
                self.buffer.append(data)
            
            self.full_sem.release() # Decrement the full slots in the buffer
    

    def __iter__(self):
        
        while True:
            self.full_sem.acquire() # Decrement the full slots in the buffer

            with self.buffer_lock:
                data = self.buffer.popleft()
            
            self.empty_sem.release() # Increment the empty slots in the buffer

            yield data
    
    def close(self):
        self.exit_event.set()
