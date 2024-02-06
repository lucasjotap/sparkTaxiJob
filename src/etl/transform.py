import os
from dotenv import load_dotenv

from pyspark.sql.functions import col, round, sum, expr, abs
from pyspark.sql.types import DoubleType, IntegerType, LongType

from spark_handler import SparkHandler
from pyspark.sql import SparkSession, DataFrame

from schema_playground import taxi_schema, vendor_schema

class Transform(object):

    def __init__(self):

        load_dotenv()
        # Inicializa a sessão do Spark
        self.spark: SparkSession = SparkHandler.create_session()
        
        # Define os caminhos dos dados de entrada e saída
        self.processed_data_path: str = os.getenv("DATA_PATH") + "/large-scale-data-processing/data/raw/*.parquet"
        self.write_to_processed_layer_data_path: str = os.getenv("DATA_PATH") + '/large-scale-data-processing/data/output/'

        # Dataframe que será lido.
        self.df: DataFrame = self.data_to_transform()

    def data_to_transform(self):
        """
        Carrega os dados de entrada usando o esquema customizado.
        """
        return self.spark.read.schema(taxi_schema).parquet(self.processed_data_path)
        
    def transfomation_action(self) -> DataFrame:
        """
        Realiza a transformação com group by sobre o total de preços por companhia de taxi.
        """
        df: DataFrame = self.df
        # Comente ou descomente a linha abaixo conforme a necessidade de conversão do tipo VendorID
        # df = df.withColumn("VendorID", col("VendorID").cast(LongType()))

        # Realiza a agregação por VendorID, somando a coluna fare_amount convertida para double
        result_df = (
            df
            .groupBy('VendorID')
            .agg(sum(col('fare_amount').cast('double')).alias('total_fare'))
        )
        return result_df

    def data_to_transform_two(self) -> DataFrame:
        """
        Realiza análise por duração de viagem.
        """
        df = self.df 

        result_df = (
            df.filter((df.fare_amount > 200) & (df.fare_amount < 300))
            .orderBy('fare_amount')
            )

        result_df = (
            result_df
            .withColumn("time_difference", expr("unix_timestamp(tpep_pickup_time) - unix_timestamp(tpep_dropoff_time)")))

        result_df = result_df.withColumn("trip_duration_minutes", abs(col("time_difference")))

        result_df  = result_df.select('VendorID', 
            'tpep_pickup_datetime', 
            'tpep_dropoff_datetime', 
            'passenger_count', 
            'trip_distance', 
            'PULocationID', 
            'DOLocationID', 
            'payment_type', 
            'fare_amount', 
            'tip_amount', 
            'total_amount', 
            'trip_duration_minutes')

        return result_df

    def data_to_transform_three(self) -> DataFrame:
        """
        Junta tabelas vendor e taxi_trips na VendorID.
        """
        df = self.df 

        result_df  = df.select('VendorID', 
            'tpep_pickup_datetime', 
            'tpep_dropoff_datetime', 
            'passenger_count', 
            'trip_distance', 
            'payment_type', 
            'fare_amount', 
            'tip_amount', 
            'total_amount'
            )

        vendor_mapping = [
            (1, "Creative Mobile Tech, LLC"),
            (2, "Verifone Inc")]

        vendor_df = self.spark.createDataFrame(vendor_mapping, schema=vendor_schema)

        result_df = df.join(vendor_df, on="VendorID", how="left")

        result_df.write.mode('overwrite').parquet('/home/lucas/Desktop/Python/large-scale-data-processing/data/output/joined_table_a')

        return result_df


    def output_data(self):
        """
        Escreve os dados transformados na camada de saída.
        """
        df = self.transfomation_action()
        df.write.mode('overwrite').parquet(self.write_to_processed_layer_data_path)

if __name__ == "__main__":
    # Instancia a classe de transformação e executa a saída de dados
    transform: Transform = Transform()
    transform.data_to_transform_three()
