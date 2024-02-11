from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampNTZType, StringType, DoubleType

from dataclasses import dataclass

@dataclass
class TaxiTrip:
	VendorID: IntegerType()
	tpep_pickup_datetime: TimestampNTZType()
	tpep_dropoff_datetime: TimestampNTZType()
	passenger_count: LongType()
	trip_distance: DoubleType()
	RatecodeID: LongType()
	store_and_fwd_flag: StringType()
	PULocationID: IntegerType()
	DOLocationID: IntegerType()
	payment_type: LongType()
	fare_amount: DoubleType()
	extra: DoubleType()
	mta_tax: DoubleType()
	tip_amount: DoubleType()
	tolls_amount: DoubleType()
	improvement_surcharge: DoubleType()
	total_amount: DoubleType()
	congestion_surcharge: DoubleType()
	Airport_fee: DoubleType()

@dataclass
class Vendors:
	VendorID: LongType() 
	company_name: StringType()

taxi_schema = StructType([
		StructField(field.name, field.type, True) for field in TaxiTrip.__dataclass_fields__.values()
	])

vendor_schema = StructType([
		StructField(field.name, field.type, False) for field in Vendors.__dataclass_fields__.values()
	])
print(vendor_schema)