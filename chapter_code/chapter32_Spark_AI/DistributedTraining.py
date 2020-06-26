# Databricks notebook source
import torch
import torch.nn as nn
import torch.nn.functional as F

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x)

# COMMAND ----------

net =Net()
net

# COMMAND ----------

# Setting training parameters
batch_size = 100
num_epochs = 5
momentum = 0.5
log_interval = 100

# COMMAND ----------

def train_one_epoch(model, device, data_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(data_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(data_loader) * len(data),
                100. * batch_idx / len(data_loader), loss.item()))

# COMMAND ----------

from time import time
import os
PYTORCH_DIR = '/dbfs/ml/horovod_pytorch'
LOG_DIR = os.path.join(PYTORCH_DIR, str(time()), 'MNISTDemo')
os.makedirs(LOG_DIR)

# COMMAND ----------

def save_checkpoint(model, optimizer, epoch):
  filepath = LOG_DIR + '/checkpoint-{epoch}.pth.tar'.format(epoch=epoch)
  state = {
    'model': model.state_dict(),
    'optimizer': optimizer.state_dict(),
  }
  torch.save(state, filepath)

# COMMAND ----------

import torch.optim as optim
from torchvision import datasets, transforms

def train(learning_rate):
  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

  train_dataset = datasets.MNIST(
    'data', 
    train=True,
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]))
  data_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

  model = Net().to(device)

  optimizer = optim.SGD(model.parameters(), lr=learning_rate, momentum=momentum)

  for epoch in range(1, num_epochs + 1):
    train_one_epoch(model, device, data_loader, optimizer, epoch)
    save_checkpoint(model, optimizer, epoch)

# COMMAND ----------

train(learning_rate = 0.001)

# COMMAND ----------

import horovod.torch as hvd
from sparkdl import HorovodRunner 

# COMMAND ----------

def train_hvd(learning_rate):
  hvd.init()  # Initialize Horovod.
  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
  
  if device.type == 'cuda':
    # Horovod: pin GPU to local rank.
    torch.cuda.set_device(hvd.local_rank())

  train_dataset = datasets.MNIST(
    root='data-%d'% hvd.rank(),  # Use different root directory for each worker to avoid race conditions.
    train=True, 
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
  )

  from torch.utils.data.distributed import DistributedSampler
  
  # Configure the sampler such that each worker obtains a distinct sample of input dataset.
  train_sampler = DistributedSampler(train_dataset, num_replicas=hvd.size(), rank=hvd.rank())
  # Use trian_sampler to load a different sample of data on each worker.
  train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size, sampler=train_sampler)

  model = Net().to(device)
  
  # Effective batch size in synchronous distributed training is scaled by the number of workers.
  # An increase in learning rate compensates for the increased batch size.
  optimizer = optim.SGD(model.parameters(), lr=learning_rate * hvd.size(), momentum=momentum)

  # Wrap the optimizer with Horovod's DistributedOptimizer.
  optimizer = hvd.DistributedOptimizer(optimizer, named_parameters=model.named_parameters())
  
  # Broadcast initial parameters so all workers start with the same parameters.
  hvd.broadcast_parameters(model.state_dict(), root_rank=0)

  for epoch in range(1, num_epochs + 1):
    train_one_epoch(model, device, train_loader, optimizer, epoch)
    # Only save checkpoints on the first worker.
    if hvd.rank() == 0:
      save_checkpoint(model, optimizer, epoch)


# COMMAND ----------

#os.environ['HOROVOD_TIMELINE'] = "E:\\testProject" 
os.environ['HOROVOD_TIMELINE'] = "/dbfs/ml/timeline" 
hr = HorovodRunner(np=2) # We assume cluster consists of two workers.
hr.run(train_hvd, learning_rate = 0.001)

# COMMAND ----------

print(dbutils.fs.ls("dbfs:/ml/horovod_pytorch/"))
print(dbutils.fs.ls("dbfs:/ml/timeline/"))  


# COMMAND ----------

print(dbutils.fs.ls("file:/ml/timeline/"))  


# COMMAND ----------

print(dbutils.fs.ls("dbfs:/ml/horovod_pytorch/1568465145.6853325/MNISTDemo/"))
