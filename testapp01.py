import time

from pyspark.shell import spark
import logging
logging.warning('Watch out!')  # will print a message to the console
logging.info('I told you so')  # will not print anything

from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local")\
#     .appName("testapp01")\
#     .config("spark.driver.bindAddress","localhost")\
#     .config("spark.ui.port","4044")\
#     .getOrCreate()

strings = spark.read.text("file:///mnt/sda/Spark/spark-3.0.1-bin-hadoop3.2/README.md")
strings.show(10, truncate=False)
time.sleep(5)

myRange = spark.range(100000).toDF("number")
myRange.show()

divisBy2 = myRange.where("number % 2 = 0")
print(divisBy2.count())