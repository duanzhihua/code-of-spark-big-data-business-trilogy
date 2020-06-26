# Databricks notebook source
cuda = False

# COMMAND ----------

# Enable Arrow support.
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2048")

# COMMAND ----------

import os
import shutil
import tarfile
import time
import zipfile

try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

import pandas as pd

import torch
from torch.utils.data import Dataset
from torchvision import datasets, models, transforms, utils
from torchvision.datasets.folder import default_loader  # private API

from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, FloatType
import torch.nn as nn 
import matplotlib.pyplot as plt

# COMMAND ----------

use_cuda = cuda and torch.cuda.is_available()
device = torch.device("cuda" if use_cuda else "cpu")

# COMMAND ----------

URL = "http://download.tensorflow.org/example_images/flower_photos.tgz"
input_local_dir = "/dbfs/ml/tmp/"
output_file_path = "/tmp/predictions"

# COMMAND ----------

def maybe_download_and_extract(url, download_dir):
    filename = url.split('/')[-1]
    file_path = os.path.join(download_dir, filename)

    if not os.path.exists(file_path):
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            
        file_path, _ = urlretrieve(url=url, filename=file_path)
        print()
        print("Download finished. Extracting files.")

        if file_path.endswith(".zip"):
            # Unpack the zip-file.
            zipfile.ZipFile(file=file_path, mode="r").extractall(download_dir)
        elif file_path.endswith((".tar.gz", ".tgz")):
            # Unpack the tar-ball.
            tarfile.open(name=file_path, mode="r:gz").extractall(download_dir)

        print("Done.")
    else:
        print("Data has apparently already been downloaded and unpacked.")

# COMMAND ----------

maybe_download_and_extract(url=URL, download_dir=input_local_dir)

# COMMAND ----------

local_dir = input_local_dir + 'flower_photos/'
files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(local_dir) for f in filenames if os.path.splitext(f)[1] == '.jpg']
len(files)

# COMMAND ----------

import random
random.shuffle(files)
files

# COMMAND ----------

#定义一个字典 对应编号
dict = {'sunflowers': 0, 'dandelion': 1, 'roses': 2,  'daisy': 3,'tulips':4}

 

class ImageDataset(Dataset):
  def __init__(self, paths, transform=None):
    self.paths = paths
    self.transform = transform
  def __len__(self):
    return len(self.paths)
  def __getitem__(self, index):
    image = default_loader(self.paths[index])
    classname = self.paths[index].split('/')[-2]
    target = dict[classname]
    print(image,target)
    if self.transform is not None:
      image = self.transform(image)
    return image,target
 

# COMMAND ----------

####定义一个dataloader

transform = transforms.Compose([
    transforms.Resize(224),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                       std=[0.229, 0.224, 0.225])
  ])
pdpaths_train=pd.Series(files[:200])
pdpaths_val=pd.Series(files[200:300])
images_train = ImageDataset(pdpaths_train, transform=transform)
images_val = ImageDataset(pdpaths_val, transform=transform)                            
image_datasets ={'train': images_train  ,'val': images_val}
 
dataloaders = {x: torch.utils.data.DataLoader(image_datasets[x], batch_size=500,shuffle=True,num_workers=2)   for x in ['train', 'val']}  
dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'val']}
dataset_sizes
 

# COMMAND ----------

import torchvision
import numpy as np

def imshow(inp, title=None):
    """Imshow for Tensor."""
    inp = inp.numpy().transpose((1, 2, 0))
    mean = np.array([0.485, 0.456, 0.406])
    std = np.array([0.229, 0.224, 0.225])
    inp = std * inp + mean
    inp = np.clip(inp, 0, 1)
    
    plt.imshow(inp)
    display()
    if title is not None:
        plt.title(title)
    plt.pause(0.001)  # pause a bit so that plots are updated
  

# Get a batch of training data
inputs, classes = next(iter(dataloaders['train'] ))

# Make a grid from batch
out = torchvision.utils.make_grid(inputs)

imshow(out, title=[list(dict.keys())[list(dict.values()).index(x)] for x in classes])

# COMMAND ----------

def train_model(model, criterion, optimizer, scheduler, num_epochs=25):
    since = time.time()

    best_model_wts = copy.deepcopy(model.state_dict())
    best_acc = 0.0

    for epoch in range(num_epochs):
        print('Epoch {}/{}'.format(epoch, num_epochs - 1))
        print('-' * 10)

        # Each epoch has a training and validation phase
        for phase in ['train', 'val']:
            if phase == 'train':
                model.train()  # Set model to training mode
            else:
                model.eval()   # Set model to evaluate mode

            running_loss = 0.0
            running_corrects = 0

            # Iterate over data.
            for inputs, labels in dataloaders[phase]:
                inputs = inputs.to(device)
                labels = labels.to(device)

                # zero the parameter gradients
                optimizer.zero_grad()

                # forward
                # track history if only in train
                with torch.set_grad_enabled(phase == 'train'):
                    outputs = model(inputs)
                    #_, preds = torch.max(outputs, 1)
                    p = torch.nn.functional.softmax(outputs, dim=1)
                    _, preds = torch.max(p, 1)
                    loss = criterion(outputs, labels)

                    # backward + optimize only if in training phase
                    if phase == 'train':
                        loss.backward()
                        optimizer.step()

                # statistics
                running_loss += loss.item() * inputs.size(0)
                running_corrects += torch.sum(preds == labels.data)
            if phase == 'train':
                scheduler.step()

            epoch_loss = running_loss / dataset_sizes[phase]
            epoch_acc = running_corrects.double() / dataset_sizes[phase]

            print('{} Loss: {:.4f} Acc: {:.4f}'.format(
                phase, epoch_loss, epoch_acc))

            # deep copy the model
            if phase == 'val' and epoch_acc > best_acc:
                best_acc = epoch_acc
                best_model_wts = copy.deepcopy(model.state_dict())

        print()

    time_elapsed = time.time() - since
    print('Training complete in {:.0f}m {:.0f}s'.format(
        time_elapsed // 60, time_elapsed % 60))
    print('Best val Acc: {:4f}'.format(best_acc))

    # load best model weights
    model.load_state_dict(best_model_wts)
    return model

# COMMAND ----------

import torch.optim as optim
from torch.optim import lr_scheduler
import copy


model_ft = models.resnet50(pretrained=True)
num_ftrs = model_ft.fc.in_features
model_ft.fc = nn.Linear(num_ftrs, 5)

model_ft = model_ft.to(device)

criterion = nn.CrossEntropyLoss()

# Observe that all parameters are being optimized
optimizer_ft = optim.SGD(model_ft.parameters(), lr=0.1, momentum=0.9)

# Decay LR by a factor of 0.1 every 7 epochs
exp_lr_scheduler = lr_scheduler.StepLR(optimizer_ft, step_size=7, gamma=0.1)
 

model_ft = train_model(model_ft, criterion, optimizer_ft, exp_lr_scheduler,  num_epochs=25)
model_ft 

# COMMAND ----------

bc_model_state = sc.broadcast(model_ft.state_dict())

# COMMAND ----------

def get_model_for_eval():
  """Gets the broadcasted model."""
  model = models.resnet50(pretrained=True)
  num_ftrs = model.fc.in_features
  model.fc = nn.Linear(num_ftrs, 5)
  model.load_state_dict(bc_model_state.value)
  model.eval()
  return model

# COMMAND ----------

files_df = spark.createDataFrame(
  map(lambda path: (path,), files), ["path"]
).repartition(10)  # number of partitions should be a small multiple of total number of nodes
display(files_df.limit(10))

# COMMAND ----------

def predict_batch(paths):
  transform = transforms.Compose([
    transforms.Resize(224),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                       std=[0.229, 0.224, 0.225])
  ])
  images = ImageDataset(paths, transform=transform)
  loader = torch.utils.data.DataLoader(images, batch_size=500, num_workers=2)
  model = get_model_for_eval()
   
   
  model.to(device)
  all_predictions = []
  with torch.no_grad():
    for inputs, _ in loader:
      logit = model(inputs.to(device))
      p = torch.nn.functional.softmax(logit, dim=1)
      predictions = list(p.cpu().numpy())
      
      _, preds = torch.max(p, 1)
      # Make a grid from batch
      #out = torchvision.utils.make_grid(inputs)
      #imshow(out, title=[list(dict.keys())[list(dict.values()).index(x)] for x in preds])
      
      for prediction in predictions:
        all_predictions.append(prediction)
  return pd.Series(all_predictions)


# COMMAND ----------

predictions = predict_batch(pd.Series(files[3600:]))
predictions 

# COMMAND ----------

def visualize_model(model, num_images=6):
    was_training = model.training
    model.eval()
    images_so_far = 0
    fig = plt.figure()

    with torch.no_grad():
        for i, (inputs, labels) in enumerate(dataloaders['val']):
            inputs = inputs.to(device)
            labels = labels.to(device)

            outputs = model(inputs)
            #_, preds = torch.max(outputs, 1)
            p = torch.nn.functional.softmax(outputs, dim=1)
            _, preds = torch.max(p, 1)

            for j in range(inputs.size()[0]):
                images_so_far += 1
                ax = plt.subplot(num_images//2, 2, images_so_far)
                ax.axis('off')
                #ax.set_title('predicted: {}'.format(   preds[j] ))
                ax.set_title('predicted: {}'.format(  list(dict.keys())[list(dict.values()).index(preds[j])] ))
                imshow(inputs.cpu().data[j])

                if images_so_far == num_images:
                    model.train(mode=was_training)
                    return
        model.train(mode=was_training)

# COMMAND ----------

visualize_model(get_model_for_eval())

# COMMAND ----------

predict_udf = pandas_udf(ArrayType(FloatType()), PandasUDFType.SCALAR)(predict_batch)
 

# COMMAND ----------

# Make predictions.
predictions_df = files_df.select(col('path'), predict_udf(col('path')).alias("prediction"))
predictions_df.show()
 

# COMMAND ----------

predictions_df.take(10)

# COMMAND ----------

predictions_df.write.mode("overwrite").parquet(output_file_path)

# COMMAND ----------

predictions_df.count()

# COMMAND ----------

result_df = spark.read.load(output_file_path)
display(result_df)

