# An End-to-End Example: from definitive guide of Spark

import time
from pyspark.shell import spark, sc
from pyspark.sql.functions import max
from pyspark.sql.functions import desc

# Read the .csv data file to create an initial RDD
flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("/mnt/sda/Sandbox/PythonLab/spark-demo/Data/2015-summary.csv")

# Take first 2 rows and check how it looks like
print(flightData2015.take(2), "\n")

# Sort the RDD according to the column 'count' <-- it does nothing (it is a wide tranformation)
flightData2015.sort("count")

# Play with partition configuration
# spark.conf.set("spark.sql.shuffle.partitions", "5")
print(flightData2015.sort("count").take(2), "\n")

# RDD.explain() will show the execution plan
flightData2015.sort("count").explain()

# Play with partition configuration
spark.conf.set("spark.sql.shuffle.partitions", "5")
print(flightData2015.sort("count").take(2), "\n")

# RDD.explain() will show the execution plan
flightData2015.sort("count").explain()

# Register a DF into a table
flightData2015.createOrReplaceTempView("flight_data_2015")

# Spark SQL way: a new DF is created by using Spark SQL
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

# Spark DF way: a new DF dataFrameWay is created with transformation
dataFrameWay = flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.count()

# Both explain() shows the same plan of transformation
sqlWay.explain()
dataFrameWay.explain()

# Spark DF way: to find the max value of column 'count'
print(flightData2015.select(max("count")).take(1))
print('\n')

# Spark SQL way: creating a new DF with the Spark SQL
maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")
maxSql.show()

# Spark DF way: creating a new DF with the Spark SQL
flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.sum("count")\
.withColumnRenamed("sum(count)", "destination_total")\
.sort(desc("destination_total"))\
.limit(5)\
.show()

# Physical plan
flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.sum("count")\
.withColumnRenamed("sum(count)", "destination_total")\
.sort(desc("destination_total"))\
.limit(5)\
.explain()

time.sleep(1000)