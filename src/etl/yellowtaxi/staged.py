from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, lit, monotonically_increasing_id


spark = SparkSession.builder.appName("yellowtaxi-job").getOrCreate()

df = spark.read.format("delta").load("/home/lucas/Desktop/Python/large-scale-data-processing/data/curated/yellowtaxi")

df = df.where(col("id") == 1000000)

df = df.withColumn("store_and_fwd_flag", lit("MATHEUS"))

df.write.format("parquet").save("/home/lucas/Desktop/Python/large-scale-data-processing/data/staged/yellowtaxi")
