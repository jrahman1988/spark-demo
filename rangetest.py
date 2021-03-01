from pyspark.shell import spark

myRange = spark.range(1000).toDF("number")
myRange.show()

strings = spark.read.text("file:///mnt/sda/Spark/spark-3.0.1-bin-hadoop3.2/README.md")
strings.show(10, truncate=False)