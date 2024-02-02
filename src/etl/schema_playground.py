from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampNTZType, StringType

from dataclasses import dataclass

@dataclass
class TaxiTrip:
	VendorID: IntegerType()
	tpep_pickup_datetime: TimestampNTZType()
	tpep_dropoff_datetime: TimestampNTZType()
	passenger_count: IntegerType()
	trip_distance: LongType()
	DOLocationID: IntegerType()
	payment_type: IntegerType()
	fare_amount: LongType()
	tip_amount: LongType()
	tolls_amount: LongType()
	total_amount: LongType()

@dataclass
class Vendors:
	VendorID: LongType() 
	company_name: StringType()

taxi_schema = StructType([
		StructField(field.name, field.type, True) for field in TaxiTrip.__dataclass_fields__.values()
	])

vendor_schema = StructType([
		StructField(field.name, field.type, True) for field in Vendors.__dataclass_fields__.values()
	])
