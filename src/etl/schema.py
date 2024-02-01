from pyspark.sql.types import (DoubleType, 
								LongType, 
								IntegerType, 
								TimestampNTZType, 
								StructType, 
								StructField)

custom_schema = StructType([
	StructField('VendorID', IntegerType(), True), 
	StructField('tpep_pickup_datetime', TimestampNTZType(), True), 
	StructField('tpep_dropoff_datetime', TimestampNTZType(), True), 
	StructField('passenger_count', LongType(), True), 
	StructField('trip_distance', DoubleType(), True), 
	StructField('DOLocationID', IntegerType(), True), 
	StructField('payment_type', LongType(), True), 
	StructField('fare_amount', DoubleType(), True), 
	StructField('tip_amount', DoubleType(), True), 
	StructField('tolls_amount', DoubleType(), True), 
	StructField('total_amount', DoubleType(), True),
	])
