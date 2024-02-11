from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("read").getOrCreate()

df = (spark
	.read
	.format("jdbc")
	.option("url", "jdbc:postgresql://127.0.0.1:5432/psql")
	.option("dbtable", "public.membros")
	.option("user", "postgres")
	.option("password", "sqlserver")
	.load()
	)