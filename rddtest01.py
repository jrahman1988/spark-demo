import time
from pyspark.shell import spark, sc

testFile = "/mnt/sda/Spark/spark-3.0.1-bin-hadoop3.2/RDDTESTFILE.TXT"
print("Test input file = ", testFile)


def printfunc(x):
    print('Word {} occurs {} '.format(x[0], x[1]))


# Initial RDD (rdd_0) creation using API: pyspark.SparkContext.textFile
rdd_00 = sc.textFile(testFile)
rdd_0 = rdd_00.repartition(8)

# pyspark.RDD.flatMap: Return a new RDD (rdd_1) by applying a function to all elements of rdd_0 and then flattening the results
rdd_1 = rdd_0.flatMap(lambda x: x.split())

# pyspark.RDD.map: Return a new RDD (rdd_2) by applying a function to each element of rdd_1
rdd_2 = rdd_1.map(lambda x: (x, 1))

# pyspark.RDD.reduceByKey: Merge the values for each key using an associative and commutative reduce function
rdd_3 = rdd_2.reduceByKey(lambda x, y: x + y)

# pyspark.RDD.toDebugString: A description of this RDD and its recursive dependencies for debugging.
print(rdd_3.toDebugString())

# pyspark.RDD.foreach: Applies a function to all elements of this RDD
rdd_3.foreach(printfunc)

time.sleep(1000)