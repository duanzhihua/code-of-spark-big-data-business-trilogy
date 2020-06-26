# Databricks notebook source
import gzip
import os
import tempfile

import numpy
from six.moves import urllib

import tensorflow as tf

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -rf  /dbfs/ml/MNISTDemo

# COMMAND ----------


params = {}
params['download_data_location'] = '/dbfs/ml/MNISTDemo/mnistData/'
params['tfrecord_location'] = '/dbfs/ml/MNISTDemo/mnistData/'

# COMMAND ----------

def download(directory, filename):
  """Download a file from the MNIST dataset if not already done."""
  filepath = os.path.join(directory, filename)
  if tf.gfile.Exists(filepath):
    return filepath
  if not tf.gfile.Exists(directory):
    tf.gfile.MakeDirs(directory)
  # CVDF mirror of http://yann.lecun.com/exdb/mnist/
  url = 'https://storage.googleapis.com/cvdf-datasets/mnist/' + filename + '.gz'
  _, zipped_filepath = tempfile.mkstemp(suffix='.gz')
  print('Downloading %s to %s' % (url, zipped_filepath))
  urllib.request.urlretrieve(url, zipped_filepath)
  tf.gfile.Copy(zipped_filepath, filepath)
  os.remove(zipped_filepath)
  return filepath

# COMMAND ----------

def _read32(bytestream):
  dt = numpy.dtype(numpy.uint32).newbyteorder('>')
  return numpy.frombuffer(bytestream.read(4), dtype=dt)[0]

def extract_images(f):
  """
  Extract the images into a 4D uint8 numpy array.
  """
  print('Extracting', f.name)
  with gzip.GzipFile(fileobj=f) as bytestream:
    magic = _read32(bytestream)
    if magic != 2051:
      raise ValueError('Invalid magic number %d in MNIST image file: %s' %
                       (magic, f.name))
    num_images = _read32(bytestream)
    rows = _read32(bytestream)
    cols = _read32(bytestream)
    buf = bytestream.read(rows * cols * num_images)
    data = numpy.frombuffer(buf, dtype=numpy.uint8)
    data = data.reshape(num_images, rows, cols, 1)
    return data

def extract_labels(f, one_hot=False, num_classes=10):
  """
  Extract the labels into a 1D uint8 numpy array.
  """
  print('Extracting', f.name)
  with gzip.GzipFile(fileobj=f) as bytestream:
    magic = _read32(bytestream)
    if magic != 2049:
      raise ValueError('Invalid magic number %d in MNIST label file: %s' %
                       (magic, f.name))
    num_items = _read32(bytestream)
    buf = bytestream.read(num_items)
    labels = numpy.frombuffer(buf, dtype=numpy.uint8)
    return labels

# COMMAND ----------

def load_dataset(directory, images_file, labels_file):
  """Download and parse MNIST dataset."""

  images_file = download(directory, images_file)
  labels_file = download(directory, labels_file)

  with tf.gfile.Open(images_file, 'rb') as f:
    images = extract_images(f)
    images = images.reshape(images.shape[0], images.shape[1] * images.shape[2])
    images = images.astype(numpy.float32)
    images = numpy.multiply(images, 1.0 / 255.0)
    
  with tf.gfile.Open(labels_file, 'rb') as f:
    labels = extract_labels(f)

  return images, labels

# COMMAND ----------

directory = params['download_data_location']

train_images, train_labels = load_dataset(directory, 'train-images-idx3-ubyte', 'train-labels-idx1-ubyte')
test_images, test_labels = load_dataset(directory, 't10k-images-idx3-ubyte', 't10k-labels-idx1-ubyte') 

# COMMAND ----------

from pyspark.sql.types import *
data = [(train_images[i].tolist(), int(train_labels[i])) for i in range(len(train_images))]
schema = StructType([StructField("image", ArrayType(FloatType())),
                     StructField("label", LongType())])
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

print(data)

# COMMAND ----------

df.take(1)

# COMMAND ----------

path = 'file:' + params['tfrecord_location'] + 'df-mnist.tfrecord'
num_partition = 4
df.repartition(num_partition).write.format("tfrecords").mode("overwrite").save(path)

# COMMAND ----------

display(dbutils.fs.ls(path))
