import os
from spark_handler import SparkHandler
from pyspark.sql import DataFrame

class Extract(object):
	"""
	Class to extract
	"""
	def __init__(self):
		self.spark = SparkHandler.create_session()
		self.raw_data_path = '/home/lucas/Desktop/Python/large-scale-data-processing/data/raw'
		self.list_of_parquets: List[str] = os.listdir(self.raw_data_path)

	def get_data_for_upload(self, parquet_file: str) -> None:
		"""
		Method for collecting data from parquet files and saving processed data.
		"""
		parquet_file_path = os.path.join(self.raw_data_path, parquet_file)
		df: DataFrame = self.spark.read.parquet(parquet_file_path)

		df = df.select(
			'VendorID', 
			'tpep_pickup_datetime', 
			'tpep_dropoff_datetime', 
			'passenger_count', 
			'trip_distance', 
			'DOLocationID', 
			'payment_type', 
			'fare_amount',
			'tip_amount', 
			'tolls_amount', 
			'total_amount'
			)

		df.write.mode("append").parquet('/home/lucas/Desktop/Python/large-scale-data-processing/data/processed/')

	def write_data_to_processed_layer(self):
		"""
		"""
		for parquet_file in self.list_of_parquets:
			self.get_data_for_upload(parquet_file)

if __name__ == "__main__":
	extract: Extract = Extract()
	extract.write_data_to_processed_layer()