from pyspark.sql.functions import col, round, sum
from pyspark.sql.types import DoubleType, IntegerType, LongType

from spark_handler import SparkHandler
from pyspark.sql import SparkSession, DataFrame

from schema import custom_schema

class Transform(object):

    def __init__(self):
        # Inicializa a sessão do Spark
        self.spark: SparkSession = SparkSession.builder.appName("Transformação de Dados").getOrCreate()
        
        # Define os caminhos dos dados de entrada e saída
        self.processed_data_path: str = "/large-scale-data-processing/data/raw/*.parquet"
        self.write_to_processed_layer_data_path: str = '/large-scale-data-processing/data/output/'

    def data_to_transform(self):
        """
        Carrega os dados de entrada usando o esquema customizado.
        """
        return self.spark.read.schema(custom_schema).parquet(self.processed_data_path)
        
    def transfomation_action(self):
        """
        Realiza a transformação necessária nos dados.
        """
        df = self.data_to_transform()

        # Comente ou descomente a linha abaixo conforme a necessidade de conversão do tipo VendorID
        # df = df.withColumn("VendorID", col("VendorID").cast(LongType()))

        # Realiza a agregação por VendorID, somando a coluna fare_amount convertida para double
        result_df = (
            df
            .groupBy('VendorID')
            .agg(sum(col('fare_amount').cast('double')).alias('total_fare'))
        )
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
    transform.output_data()
