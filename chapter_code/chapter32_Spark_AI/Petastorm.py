# Databricks notebook source
import os
import subprocess
import uuid

# COMMAND ----------

work_dir = os.path.join("/ml/tmp/petastorm", str(uuid.uuid4()))
dbutils.fs.mkdirs(work_dir)

def get_local_path(dbfs_path):
  return os.path.join("/dbfs", dbfs_path.lstrip("/"))

# COMMAND ----------

data_url = "https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass/mnist.bz2"
libsvm_path = os.path.join(work_dir, "mnist.bz2")
subprocess.check_output(["wget", data_url, "-O", get_local_path(libsvm_path)])

# COMMAND ----------

df = spark.read.format("libsvm") \
  .option("numFeatures", "784") \
  .load(libsvm_path)

# COMMAND ----------

df.show()

# COMMAND ----------

df.take(1)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.linalg.Vector
# MAGIC 
# MAGIC val toArray = udf { v: Vector => v.toArray }
# MAGIC spark.sqlContext.udf.register("toArray", toArray)

# COMMAND ----------

parquet_path = os.path.join(work_dir, "parquet")
df.selectExpr("toArray(features) AS features", "int(label) AS label") \
  .repartition(10) \
  .write.mode("overwrite") \
  .option("parquet.block.size", 1024 * 1024) \
  .parquet(parquet_path)

# COMMAND ----------

df.selectExpr("toArray(features) AS features", "int(label) AS label").show()

# COMMAND ----------

df.selectExpr("toArray(features) AS features", "int(label) AS label").take(1)

# COMMAND ----------

dbutils.fs.ls(parquet_path )


# COMMAND ----------

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models, layers

from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset

# COMMAND ----------

def get_model():
  model = models.Sequential()
  model.add(layers.Conv2D(32, kernel_size=(3, 3),
                          activation='relu',
                          input_shape=(28, 28, 1)))
  model.add(layers.Conv2D(64, (3, 3), activation='relu'))
  model.add(layers.MaxPooling2D(pool_size=(2, 2)))
  model.add(layers.Dropout(0.25))
  model.add(layers.Flatten())
  model.add(layers.Dense(128, activation='relu'))
  model.add(layers.Dropout(0.5))
  model.add(layers.Dense(10, activation='softmax'))
  return model

# COMMAND ----------

model = get_model()
print(model)

# COMMAND ----------

model.summary()

# COMMAND ----------

import pyarrow.parquet as pq

underscore_files = [f for f in os.listdir(get_local_path(parquet_path)) if f.startswith("_")]
pq.EXCLUDED_PARQUET_PATHS.update(underscore_files)


# COMMAND ----------

print(underscore_files)
print(pq)

# COMMAND ----------

# We use make_batch_reader to load Parquet row groups into batches.
# HINT: Use cur_shard and shard_count params to shard data in distributed training.
petastorm_dataset_url = "file://" + get_local_path(parquet_path)
print(petastorm_dataset_url)
with make_batch_reader(petastorm_dataset_url, num_epochs=100) as reader:
  dataset = make_petastorm_dataset(reader) \
    .map(lambda x: (tf.reshape(x.features, [-1, 28, 28, 1]), tf.one_hot(x.label, 10)))
  model = get_model()
  optimizer = keras.optimizers.Adadelta()
  model.compile(optimizer=optimizer,
                loss='categorical_crossentropy',
                metrics=['accuracy'])
  model.fit(dataset, steps_per_epoch=10, epochs=10)

# COMMAND ----------

dbutils.fs.ls(work_dir )

# COMMAND ----------

# Clean up the working directory.
dbutils.fs.rm(work_dir, recurse=True)
