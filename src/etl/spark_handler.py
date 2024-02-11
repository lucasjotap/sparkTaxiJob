from pyspark.sql import SparkSession

class SparkHandler(object):
	"""
	Classe lidará com a instanciação do Spark Session somente.
		-- create_session() : cria a Spark Session
	"""
	spark: SparkSession = None

	@classmethod
	def create_session(cls) -> SparkSession:

		if cls.spark is None:
			cls.spark = (
				SparkSession
				.builder
				.appName("my_spark_app")
				.getOrCreate()
				)
		return cls.spark
