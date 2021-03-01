from pyspark.shell import spark

myRange = spark.range(1000).toDF("number")
myRange.show()