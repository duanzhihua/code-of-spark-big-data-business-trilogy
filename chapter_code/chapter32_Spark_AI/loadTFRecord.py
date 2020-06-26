# Databricks notebook source
import os
import tensorflow as tf

# COMMAND ----------

tfrecord_location = '/dbfs/ml/MNISTDemo/mnistData/'
name = "train.tfrecords"
filename = os.path.join(tfrecord_location, name)

# COMMAND ----------

print(filename)
dbutils.fs.ls("file:"+tfrecord_location)

# COMMAND ----------

dataset = tf.data.TFRecordDataset(filename)


# COMMAND ----------

print(dataset)

# COMMAND ----------

def decode(serialized_example):
  """
  Parses an image and label from the given `serialized_example`.
  It is used as a map function for `dataset.map`
  """
  IMAGE_SIZE = 28
  IMAGE_PIXELS = IMAGE_SIZE * IMAGE_SIZE
  
  # 1. define a parser
  features = tf.parse_single_example(
      serialized_example,
      # Defaults are not specified since both keys are required.
      features={
          'image_raw': tf.FixedLenFeature([], tf.string),
          'label': tf.FixedLenFeature([], tf.int64),
      })

  # 2. Convert the data
  image = tf.decode_raw(features['image_raw'], tf.uint8)
  label = tf.cast(features['label'], tf.int32)
  # 3. reshape
  image.set_shape((IMAGE_PIXELS))
  return image, label

# COMMAND ----------

dataset = dataset.map(decode)

# COMMAND ----------

def normalize(image, label):
  """Convert `image` from [0, 255] -> [-0.5, 0.5] floats."""
  image = tf.cast(image, tf.float32) * (1. / 255) - 0.5
  return image, label

# COMMAND ----------

dataset = dataset.map(normalize)
batch_size = 1000
dataset = dataset.shuffle(1000 + 3 * batch_size )
dataset = dataset.repeat(2)
dataset = dataset.batch(batch_size)

# COMMAND ----------

iterator = dataset.make_one_shot_iterator()
image_batch, label_batch = iterator.get_next()

# COMMAND ----------

sess = tf.Session()
image_batch, label_batch = sess.run([image_batch, label_batch])
print(image_batch.shape)
print(label_batch.shape)

# COMMAND ----------

print(image_batch)

# COMMAND ----------

print(label_batch)
