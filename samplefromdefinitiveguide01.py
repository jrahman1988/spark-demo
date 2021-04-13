import pandas as pd
import numpy as np
from pyspark.shell import spark

# Create a Spark Dataframe (or Dataset)
myRange = spark.range(1000).toDF("number")
myRange.show()

# 2 modulous of the create df
divisBy2 = myRange.where("number % 2 = 0")
divisBy2.show()

# Default RDD partition creation
print('Number of partitions created: {}'.format(myRange.rdd.getNumPartitions()))
print('Partitioner: {}'.format(myRange.rdd.partitioner))
print('Partitions structure: {}'.format(myRange.rdd.glom().collect()))
print ('Total transactions RDD instances: {}'.format(myRange.rdd.count()))

# Re-partitioned increased to 8
repartitioned = myRange.repartition(8)
print('Number of partitions: {}'.format(repartitioned.rdd.getNumPartitions()))
print('Partitions structure: {}'.format(repartitioned.rdd.glom().collect()))
print ('Total repartitioned RDD instances: {}'.format(repartitioned.rdd.count()))
repartitioned.show()

# Re-partitioning by specifying the column
repartitioned = myRange.repartition('number')
print('Number of partitions: {}'.format(repartitioned.rdd.getNumPartitions()))
print('Partitions structure: {}'.format(repartitioned.rdd.glom().collect()))
print ('Total repartitioned RDD instances based on country column: {}'.format(repartitioned.rdd.count()))
repartitioned.show()

coalesced = myRange.coalesce(2)
print('Number of partitions: {}'.format(coalesced.rdd.getNumPartitions()))
print('Partitions structure: {}'.format(coalesced.rdd.glom().collect()))
print ('Total repartitioned RDD instances based on colesce: {}'.format(coalesced.rdd.count()))
coalesced.show()