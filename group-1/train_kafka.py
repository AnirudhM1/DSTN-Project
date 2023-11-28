import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
import torchvision

from data import KafkaBatcherDataPipe, KafkaDataPipe, KafkaDeserializerDataPipe
from kafka import KafkaConsumer, KafkaProducer

import os

from tqdm.auto import tqdm

# import wandb

NUM_BATCHES = 20
LEARNING_RATE = 1e-3
BATCH_SIZE = 64

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


config = {
    "Num Batches": NUM_BATCHES,
    "Learning Rate": LEARNING_RATE,
    "Batch Size": BATCH_SIZE,
}

# wandb.init(project="kafka", config=config)


consumer = KafkaConsumer(topic_name="stream", server_name="localhost")
producer = KafkaProducer(topic_name="req", server_name="localhost")
dp = KafkaDataPipe(consumer)
dp = KafkaDeserializerDataPipe(dp)
dp = KafkaBatcherDataPipe(dp, consumer, producer, batch_size=BATCH_SIZE)

train_loader = DataLoader(dp, batch_size=None, num_workers=0)
model = torchvision.models.resnet18()
model.fc = nn.Linear(512, 1, bias=True)
optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

model = model.to(device)


@torch.no_grad()
def accuracy(y_pred: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    y_pred = torch.sigmoid(y_pred)
    y_pred = (y_pred > 0.5).float()
    return (y_pred == y).float().mean()


progress_bar = tqdm(range(NUM_BATCHES))

model.train()

batch_count = 0
for batch in train_loader:
    batch_count += 1
    x, y = batch
    x = x.to(device) / 255.  # (batch_size, 3, 64, 64)
    y = y.to(device).unsqueeze(-1)  # (batch_size, 1)

    optimizer.zero_grad()
    y_pred = model(x)  # (batch_size, 1)

    loss = F.binary_cross_entropy_with_logits(y_pred, y.float())
    loss.backward()
    acc = accuracy(y_pred, y)

    optimizer.step()
    progress_bar.update(1)

    if batch_count == NUM_BATCHES:
        break

    # wandb.log(
    #     {
    #         "train_loss": loss.detach().cpu().item(),
    #         "train_accuracy": acc.detach().cpu().item(),
    #     }
    # )

    print(f"Loss: {loss.detach().cpu().item():.4f}, Accuracy: {acc.detach().cpu().item():.4f}")
