import os
import psycopg2
import pandas as pd

from sqlalchemy import create_engine
from spark_handler import SparkHandler

class Load(object):

	def __init__(self):
		self.dbname='taxi_data'
		self.user='postgres'
		self.password='sqlserver'
		self.host='localhost'
		self.port='5432'

		self.conn = psycopg2.connect(
			dbname=self.dbname,
			user=self.user,
			password=self.password,
			host='localhost',
			port='5432'
		)

		self.cur = self.conn.cursor()
		self.spark = SparkHandler.create_session()

	def define_them_partitions(self):
		df = self.spark.read.parquet("/home/lucas/Desktop/Python/large-scale-data-processing/data/output/joined_table_b")
		# df = (
		# 	df.withColumn('','')
		# 	.withColumn('','')
		# 	.withColumn('','')
		# 	.withColumn('','')
		# 	.withColumn('','')
		# 	.withColumn('','')
		# 	)
		df = df.repartition(90)
		df.write.mode("overwrite").parquet("/home/lucas/Desktop/Python/large-scale-data-processing/data/output/joined_table_c")
		print("\nRepartition job finished\n")

	def create_engine(self):
		return create_engine(f'postgresql://{self.user}:{self.password}@localhost:5432/{self.dbname}')

	def create_table(self):

		query = """
			CREATE TABLE IF NOT EXISTS taxi_trips (
			    vendorid INTEGER,
			    tpep_pickup_datetime TIMESTAMP,
			    tpep_dropoff_datetime TIMESTAMP,
			    passenger_count INTEGER,
			    trip_distance DECIMAL,
			    RatecodeID INTEGER,
			    store_and_fwd_flag VARCHAR(1),
			    pulocationID INTEGER,
			    dolocationID INTEGER,
			    payment_type INTEGER,
			    fare_amount DECIMAL,
			    extra DECIMAL,
			    mta_tax DECIMAL,
			    tip_amount DECIMAL,
			    tolls_amount DECIMAL,
			    improvement_surcharge DECIMAL,
			    total_amount DECIMAL,
			    congestion_surcharge DECIMAL,
			    airport_fee DECIMAL,
			    company_name VARCHAR(255)
			);
			"""
		self.cur.execute(query)
		self.conn.commit()

	def load_data(self):
		# self.create_table()
		# self.define_them_partitions()

		parquet_files_list = os.listdir(path='/home/lucas/Desktop/Python/large-scale-data-processing/data/output/joined_table_c')
		parquet_files = [file for file in parquet_files_list if file.endswith('.parquet')]
		engine = self.create_engine()

		for parquet in parquet_files:
			df = pd.read_parquet(f"/home/lucas/Desktop/Python/large-scale-data-processing/data/output/joined_table_c/{parquet}")
			print(df)
			
			df.to_sql('taxi_trips', engine, if_exists='append', index=False)
			self.conn.commit()
			self.close_sessions()
			break

	def close_sessions(self):
		self.cur.close()
		self.conn.close()

if __name__ == "__main__":
	ld = Load()
	ld.load_data()