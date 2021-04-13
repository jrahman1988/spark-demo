# Example on Lazy Evaluation from:
# https://medium.com/analytics-vidhya/being-lazy-is-useful-lazy-evaluation-in-spark-1f04072a3648

import time
from pyspark.shell import spark, sc, sqlContext
import pandas as pd

# # Data file
# testFile = "/mnt/sda/Spark/spark-3.0.1-bin-hadoop3.2/RDDTESTFILE.TXT"
#
# # create a sample list
# my_list = [i for i in range(0,12)]
#
# # Creation of RDD from the list using pyspark.SparkContext.parallelize()
# rdd_0 = sc.parallelize(my_list, 5)
# print(rdd_0.toDebugString())
# print("Number of partitions: {}".format(rdd_0.getNumPartitions()))
# print("Partitions structure: {} ".format(rdd_0.glom().collect()), "\n")
#
# # Creation of RDD using API: pyspark.SparkContext.textFile()
# rdd_1 = sc.textFile(testFile,10)
# print(rdd_1.toDebugString())
# print("Number of partitions: {}".format(rdd_1.getNumPartitions()))
# print("Partitions structure: {}".format(rdd_1.glom().collect()), "\n")
#
# # Creation of RDD from another RDD thru transformation map() method (add value 4 to each number)
# rdd_00 = rdd_0.map(lambda x : x+4)
# print(rdd_00.toDebugString())
# print("Number of partitions: {}".format(rdd_00.getNumPartitions()))
# print("Partitions structure: {}".format(rdd_00.glom().collect()), "\n")

# Creation of RDD from a panda's DF
# Create Pandas DataFrame from a dictionary
data = {"Name":['Roshan', 'Hossam', 'Bala', 'Marcel', 'Deepak'], "Membership Due":[100, 200, 300, 400, 500]}
pandasDf0 = pd.DataFrame(data)
print(" Here is the Pandas DF created form a dictionary \n {}".format(pandasDf0.head()))

# Now convert the Panda's DF to Spark DF accommodate with 7 partitions using sqlContext.createDataFrame(df) method
sparkDf0 = sqlContext.createDataFrame(pandasDf0).repartition(7)
rdd_000 = sparkDf0.rdd.map(list)

print("\n Number of partitions: {}".format(rdd_000.getNumPartitions()))
print("\n Converted RDD rdd_000 (data shown inside partitions): \n {}".format(rdd_000.glom().collect()))
print("\n Converted RDD rdd_000 (data shown as a flat list): \n {}".format(rdd_000.collect()))

# # get the RDD Lineage
# print(rdd_1.toDebugString(), "\n")
#
# # add value 20 each number
# rdd_2 = rdd_1.map(lambda x : x+20)
#
# # RDD Object
# print(rdd_2)
#
# # get the RDD Lineage
# print(rdd_2.toDebugString(), "\n")

time.sleep(1000)