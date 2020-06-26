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
from torchvision import datasets, models, transforms
from torchvision.datasets.folder import default_loader  # private API

from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, FloatType


# COMMAND ----------

use_cuda = cuda and torch.cuda.is_available()
device = torch.device("cuda" if use_cuda else "cpu")

# COMMAND ----------

URL = "http://download.tensorflow.org/example_images/flower_photos.tgz"
input_local_dir = "/dbfs/ml/tmp/flower/"
output_file_path = "/tmp/predictions"

# COMMAND ----------

bc_model_state = sc.broadcast(models.resnet50(pretrained=True).state_dict())

# COMMAND ----------

def get_model_for_eval():
  """Gets the broadcasted model."""
  model = models.resnet50(pretrained=True)
  model.load_state_dict(bc_model_state.value)
  model.eval()
  return model

# COMMAND ----------

def maybe_download_and_extract(url, download_dir):
    filename = url.split('/')[-1]
    file_path = os.path.join(download_dir, filename)
    print(file_path)
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

print(os.path.join(input_local_dir,URL.split('/')[-1]))

# COMMAND ----------

print(dbutils.fs.ls("dbfs:/ml/tmp/flower_photos/"))

# COMMAND ----------

print(dbutils.fs.ls("dbfs:/ml/tmp/flower_photos/daisy/"))

# COMMAND ----------

local_dir = input_local_dir + 'flower_photos/'
files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(local_dir) for f in filenames if os.path.splitext(f)[1] == '.jpg']
len(files)

# COMMAND ----------

files_df = spark.createDataFrame(
  map(lambda path: (path,), files), ["path"]
).repartition(10)  # number of partitions should be a small multiple of total number of nodes
display(files_df.limit(10))

# COMMAND ----------

class ImageDataset(Dataset):
  def __init__(self, paths, transform=None):
    self.paths = paths
    self.transform = transform
  def __len__(self):
    return len(self.paths)
  def __getitem__(self, index):
    image = default_loader(self.paths[index])
    if self.transform is not None:
      image = self.transform(image)
    return image

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
  loader = torch.utils.data.DataLoader(images, batch_size=500, num_workers=8)
  model = get_model_for_eval()
  model.to(device)
  all_predictions = []
  with torch.no_grad():
    for batch in loader:
      predictions = list(model(batch.to(device)).cpu().numpy())
      for prediction in predictions:
        all_predictions.append(prediction)
  return pd.Series(all_predictions)

# COMMAND ----------

predictions = predict_batch(pd.Series(files[:200]))

# COMMAND ----------

predict_udf = pandas_udf(ArrayType(FloatType()), PandasUDFType.SCALAR)(predict_batch)

# COMMAND ----------

# Make predictions.
predictions_df = files_df.select(col('path'), predict_udf(col('path')).alias("prediction"))
predictions_df.write.mode("overwrite").parquet(output_file_path)

# COMMAND ----------

result_df = spark.read.load(output_file_path)
display(result_df)
