from pyspark.sql.functions import col, round, sum
from pyspark.sql.types import DoubleType, IntegerType, LongType

from pyspark.sql import SparkSession, DataFrame
from spark_handler import SparkHandler

from schema import custom_schema

class Transform(object):

	def __init__(self):
		self.spark: SparkSession = SparkHandler.create_session()
		self.processed_data_path: str = "/home/lucas/Desktop/Python/large-scale-data-processing/data/raw/*.parquet"
		self.write_to_processed_layer_data_path: str = '/home/lucas/Desktop/Python/large-scale-data-processing/data/output/'

	def data_to_transform(self):
		"""
		"""
		return self.spark.read.schema(custom_schema).parquet(self.processed_data_path)
		
	def transfomation_action(self):
		"""
		"""
		df = self.data_to_transform()

		# df = df.withColumn("VendorID", col("VendorID").cast(LongType()))

		result_df = (
		    df
		    .groupBy('VendorID')
		    .agg(sum(col('fare_amount')
		    .cast('double'))
		    .alias('total_fare'))
		    )
		return result_df

	def output_data(self):
		"""
		"""
		df = self.transfomation_action()
		df.write.mode('overwrite').parquet(self.write_to_processed_layer_data_path)

if __name__ == "__main__":
	transform: Transform = Transform()
	transform.output_data()
