"""run.py"""
from os import environ
import pyspark
from pyspark import SparkContext

logFile = environ['SPARK_HOME'] + "/README.md"
print ("Log file = ", logFile)
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" %(numAs, numBs))

print ("SparkContext version = ", sc.version)
print ("Python version = ", sc.pythonVer)
print ("Master URL to connect to = ", sc.master)
print ("Path where Spark is installed on worker node = ", str(sc.sparkHome))
print ("Spark user name = ", str(sc.sparkUser()))
print ("Return application name = ", sc.appName)
print ("Default parallelism = ", sc.defaultParallelism)