from pyspark.sql.functions import col, round, sum
from pyspark.sql.types import DoubleType, IntegerType

from pyspark.sql import SparkSession, DataFrame
from spark_handler import SparkHandler

from schema import custom_schema

class Transform(object):

	def __init__(self):
		self.spark: SparkSession = SparkHandler.create_session()
		self.processed_data_path: str = "/home/lucas/Desktop/Python/large-scale-data-processing/data/processed/.part-00000-021fa0a4-628f-45e8-8ebc-ced9268f79df-c000.snappy.parquet.crc"
		self.write_to_processed_layer_data_path: str = '/home/lucas/Desktop/Python/large-scale-data-processing/data/output/'

	def data_to_transform(self):
		"""
		"""
		return self.spark.read.schema(custom_schema).parquet(self.processed_data_path)
		
	def transfomation_action(self):
		"""
		"""
		df = self.data_to_transform()

		df = df.withColumn("VendorID", col("VendorID").cast(IntegerType()))

		result_df = (
		    df
		    .groupBy("VendorID")  # GROUP BY VendorID
		    .agg(round(sum("fare_amount"), 2).alias("rounded_sum_fare_amount"))  # ROUND(SUM(fare_amount), 2) and alias the result
		    .select("VendorID", "rounded_sum_fare_amount")  # SELECT VendorID and the rounded sum
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
