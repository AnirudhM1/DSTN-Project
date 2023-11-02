import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader

import os
import json
from typing import Tuple, List, Dict

from tqdm.auto import tqdm

import torchvision
import torchvision.transforms as T
from torchvision.io import read_image

import wandb


NUM_EPOCHS = 10
LEARNING_RATE = 1e-3
BATCH_SIZE = 256
NUM_WORKERS = 3


config = {
    'num_epochs': NUM_EPOCHS,
    'learning_rate': LEARNING_RATE,
    'batch_size': BATCH_SIZE,
    'num_workers': NUM_WORKERS
}
wandb.init(project='baselines', config=config)

class CelebaDataset(Dataset):
    def __init__(self, img_dir: str, label_path: str, partition_path: str, split='train', transform=None):
        self.img_dir = img_dir
        with open(label_path, 'r') as f:
            self.labels = json.load(f)
        with open(partition_path, 'r') as f:
            partition = json.load(f)
        self.img_names = partition[split]
        
        if transform is None:
            self.transform = T.Compose([
                T.Resize((64, 64), antialias=True)
            ])
        else:
            self.transform = transform
    
    def __len__(self):
        return len(self.img_names)

    def __getitem__(self, idx: int):
        img_name = self.img_names[idx]
        label = self.labels[img_name]
        img_path = os.path.join(self.img_dir, img_name)
        img = read_image(img_path) / 255.
        img = self.transform(img)
        return img, float(label)

img_dir = 'datasets/img_align_celeba/img_align_celeba'
label_path = 'datasets/labels.json'
partition_path = 'datasets/partition.json'

train_dataset = CelebaDataset(img_dir, label_path, partition_path, split='train')
val_dataset = CelebaDataset(img_dir, label_path, partition_path, split='val')
test_dataset = CelebaDataset(img_dir, label_path, partition_path, split='test')


device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
train_dataloader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True, num_workers=NUM_WORKERS, pin_memory=True)
val_dataloader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=True, num_workers=NUM_WORKERS, pin_memory=True)
test_dataloader = DataLoader(test_dataset, batch_size=BATCH_SIZE, shuffle=False, num_workers=NUM_WORKERS, pin_memory=True)


model = torchvision.models.resnet18()
model.fc = nn.Linear(512, 1, bias=True)
optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

model = model.to(device)

@torch.no_grad()
def accuracy(y_pred: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    y_pred = torch.sigmoid(y_pred)
    y_pred = (y_pred > 0.5).float()
    return (y_pred == y).float().mean()

history = {
    'train_loss': [],
    'train_accuracy': [],
    'val_loss': [],
    'val_accuracy': []
}

training_step = 0
validation_step = 0

progress_bar = tqdm(range(NUM_EPOCHS * (len(train_dataloader) + len(val_dataloader))))
for epoch in range(1, NUM_EPOCHS + 1):
    model.train()
    train_loss = []
    train_accuracy = []

    for batch in train_dataloader:
        x, y = batch
        x = x.to(device) # (batch_size, 3, 64, 64)
        y = y.to(device).unsqueeze(-1) # (batch_size, 1)

        optimizer.zero_grad()
        y_pred = model(x) # (batch_size, 1)

        loss = F.binary_cross_entropy_with_logits(y_pred, y)
        loss.backward()
        train_loss.append(loss.detach())
        acc = accuracy(y_pred, y)
        train_accuracy.append(acc)

        optimizer.step()
        progress_bar.update(1)
        training_step += 1

        wandb.log({
            'train_loss': loss.detach().cpu().item(),
            'train_accuracy': acc.detach().cpu().item(),
            'training_step': training_step
        })
    
    train_loss = torch.stack(train_loss).mean().cpu().item()
    train_accuracy = torch.stack(train_accuracy).mean().cpu().item()

    model.eval()
    val_loss = []
    val_accuracy = []

    with torch.no_grad():
        for batch in val_dataloader:
            x, y = batch
            x = x.to(device)
            y = y.to(device).unsqueeze(-1)

            y_pred = model(x)

            loss = F.binary_cross_entropy_with_logits(y_pred, y)
            val_loss.append(loss)

            acc = accuracy(y_pred, y)
            val_accuracy.append(acc)

            progress_bar.update(1)
            progress_bar.set_postfix({'Loss': loss.item(), 'Accuracy': acc.item()})
            validation_step += 1

            wandb.log({
                'val_loss': loss.detach().cpu().item(),
                'val_accuracy': acc.detach().cpu().item(),
                'validation_step': validation_step
            })
    
    val_loss = torch.stack(val_loss).mean().cpu().item()
    val_accuracy = torch.stack(val_accuracy).mean().cpu().item()
    progress_bar.set_postfix({'Accuracy:': val_accuracy})

    history['train_loss'].append(train_loss)
    history['train_accuracy'].append(train_accuracy)
    history['val_loss'].append(val_loss)
    history['val_accuracy'].append(val_accuracy)


    print(f'Epoch: [{epoch}/{NUM_EPOCHS}] | Train Loss: {train_loss:.4f} | Train Accuracy: {train_accuracy:.4f} | Val Loss: {val_loss:.4f} | Val Accuracy: {val_accuracy:.4f}')


