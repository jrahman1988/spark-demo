"""run.py"""
# from os import environ
# import pyspark
# from pyspark import SparkContext
#
# logFile = environ['SPARK_HOME'] + "/README.md"
# print ("Log file = ", logFile)
# sc = SparkContext("local", "Simple App")
# logData = sc.textFile(logFile).cache()
#
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()
#
# print("Lines with a: %i, lines with b: %i" %(numAs, numBs))
#
# print ("SparkContext version = ", sc.version)
# print ("Python version = ", sc.pythonVer)
# print ("Master URL to connect to = ", sc.master)
# print ("Path where Spark is installed on worker node = ", str(sc.sparkHome))
# print ("Spark user name = ", str(sc.sparkUser()))
# print ("Return application name = ", sc.appName)
# print ("Default parallelism = ", sc.defaultParallelism)

from pyspark.shell import spark
strings = spark.read.text("file:///mnt/sda/Spark/spark-3.0.1-bin-hadoop3.2/README.md")
strings.show(10, truncate=False)

myRange = spark.range(1000).toDF("number")
myRange.show()