import os
from dotenv import load_dotenv

from spark_handler import SparkHandler

class DataLoader(object):

	def __init__(self):
		load_dotenv()
		# Register PostgreSQL JDBC driver
		self.spark = SparkHandler.create_session()
		self.jdbc_url = "jdbc:postgresql://127.0.0.1:5432/taxi_data"
		self.table_name = "taxi_trips"
		self.properties = {
	    "user_name": "postgres",
	    "password": "sqlserver"
		}

	def create_table(self):

		query = """
			CREATE TABLE IF NOT EXISTS taxi_trips (
			    VendorID INTEGER,
			    tpep_pickup_datetime TIMESTAMP,
			    tpep_dropoff_datetime TIMESTAMP,
			    passenger_count INTEGER,
			    trip_distance DECIMAL,
			    RatecodeID INTEGER,
			    store_and_fwd_flag VARCHAR(1),
			    PULocationID INTEGER,
			    DOLocationID INTEGER,
			    payment_type INTEGER,
			    fare_amount DECIMAL,
			    extra DECIMAL,
			    mta_tax DECIMAL,
			    tip_amount DECIMAL,
			    tolls_amount DECIMAL,
			    improvement_surcharge DECIMAL,
			    total_amount DECIMAL,
			    congestion_surcharge DECIMAL,
			    Airport_fee DECIMAL,
			    company_name VARCHAR(255)
			);
		"""

		(

		self.spark
		.sql(query)
	    .write
	    .format("jdbc")
	    .option("url", self.jdbc_url)
	    .option("dbtable", "taxi_data.taxi_trips")
	    .option("user", self.properties["user_name"])
	    .option("password", self.properties["password"])
	    .save()
	    )

	def load_data_into_dw(self):
		df = self.spark.read.parquet(os.getenv("DATA_PATH") \
			+ "/large-scale-data-processing/data/output/joined_table_a")

		(
		df.write.format("jdbc")
	    .option("url", self.jdbc_url)
	    .option("dbtable", "taxi_data.taxi_trips")
	    .option("user", self.properties.get('user_name'))
	    .option("password", self.properties.get('password'))
	    .save()
	    )

		print("Data loaded to dw!")

if __name__ == "__main__":
	dl = DataLoader()
	dl.create_table()
	dl.load_data_into_dw()