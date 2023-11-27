import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
import torchvision

from data import KafkaBatcherDataPipe, KafkaDataPipe, KafkaDeserializerDataPipe
from kafka import KafkaConsumer, KafkaProducer

import os

from tqdm.auto import tqdm

import wandb

NUM_EPOCHS = 10
LEARNING_RATE = 1e-3
BATCH_SIZE = 256

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


config = {
    "num_epochs": NUM_EPOCHS,
    "learning_rate": LEARNING_RATE,
    "batch_size": BATCH_SIZE,
}

wandb.init(project="kafka", config=config)


consumer = KafkaConsumer(
    "celeba",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: x.decode("utf-8"),
)
dp = KafkaDeserializerDataPipe(consumer)
dp = KafkaDataPipe(dp)
dp = KafkaBatcherDataPipe(dp, batch_size=BATCH_SIZE)

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


progress_bar = tqdm(range(NUM_EPOCHS))

model.train()

for batch in train_loader:
    x, y = batch
    x = x.to(device)  # (batch_size, 3, 64, 64)
    y = y.to(device).unsqueeze(-1)  # (batch_size, 1)

    optimizer.zero_grad()
    y_pred = model(x)  # (batch_size, 1)

    loss = F.binary_cross_entropy_with_logits(y_pred, y)
    loss.backward()
    acc = accuracy(y_pred, y)

    optimizer.step()
    progress_bar.update(1)

    wandb.log(
        {
            "train_loss": loss.detach().cpu().item(),
            "train_accuracy": acc.detach().cpu().item(),
        }
    )

