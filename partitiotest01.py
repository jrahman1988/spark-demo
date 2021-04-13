import pandas as pd
import numpy as np
from pyspark.shell import spark

length = 100
names = np.random.choice(['Bob', 'James', 'Marek', 'Johannes', None], length)
amounts = np.random.randint(0, 1000000, length)
country = np.random.choice(
	['United Kingdom', 'Poland', 'USA', 'Germany', None],
	length
)
df = pd.DataFrame({'name': names, 'amount': amounts, 'country': country})
print(df.head())

# Default RDD partition creation
transactions = spark.createDataFrame(df)
print('Number of partitions: {}'.format(transactions.rdd.getNumPartitions()))
print('Partitioner: {}'.format(transactions.rdd.partitioner))
print('Partitions structure: {}'.format(transactions.rdd.glom().collect()))
print ('Total transactions RDD instances: {}'.format(transactions.rdd.count()))

# Re-partitioned increased to 8
repartitioned = transactions.repartition(8)
print('Number of partitions: {}'.format(repartitioned.rdd.getNumPartitions()))
print('Partitions structure: {}'.format(repartitioned.rdd.glom().collect()))
print ('Total repartitioned RDD instances: {}'.format(repartitioned.rdd.count()))

# Re-partitioning by specifying the column
repartitioned = transactions.repartition('country')
print('Number of partitions: {}'.format(repartitioned.rdd.getNumPartitions()))
print('Partitions structure: {}'.format(repartitioned.rdd.glom().collect()))
print ('Total repartitioned RDD instances based on country column: {}'.format(repartitioned.rdd.count()))