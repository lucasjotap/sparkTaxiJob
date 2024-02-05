from delta.tables import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, lit, monotonically_increasing_id


spark = SparkSession.builder.appName("yellowtaxi-job").getOrCreate()


updates = spark.read.parquet("/home/lucas/Desktop/Python/large-scale-data-processing/data/staged/yellowtaxi")


target = DeltaTable.forPath(spark, "/home/lucas/Desktop/Python/large-scale-data-processing/data/curated/yellowtaxi")

(
	target.alias("target")
	.merge(
		updates.alias("updates"),  "updates.id = target.id"
	)
	.whenMatchedUpdateAll()
	.whenNotMatchedInsertAll()
	.execute()
)
