import os
from typing import List
from spark_handler import SparkHandler

from pyspark.sql import DataFrame
from pyspark.sql.functions import col 
from pyspark.sql.types import IntegerType

from schema import custom_schema

cclass Extract(object):
    """
    Classe para extração de dados.
    """
    def __init__(self):
        # Cria a sessão do Spark
        self.spark = SparkHandler.create_session()
        self.raw_data_path = '/home/lucas/Desktop/Python/large-scale-data-processing/data/raw'
        # Obtém a lista de arquivos parquet no caminho fornecido
        self.list_of_parquets: List[str] = os.listdir(self.raw_data_path)

    def get_data_for_upload(self, parquet_file: str) -> None:
        """
        Método para coletar dados de arquivos parquet e salvar dados processados.
        """
        parquet_file_path = os.path.join(self.raw_data_path, parquet_file)
        # Lê o arquivo parquet usando o esquema customizado
        df: DataFrame = self.spark.read.schema(custom_schema).parquet(parquet_file_path)
        # Converte a coluna 'VendorID' para tipo Integer
        df = df.withColumn('VendorID', col("VendorID").cast(IntegerType()))

        # Seleciona as colunas desejadas
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

        # Escreve os dados processados no modo "append"
        df.write.mode("append").parquet('/home/lucas/Desktop/Python/large-scale-data-processing/data/processed/')

    def write_data_to_processed_layer(self):
        """
        Método para iterar sobre a lista de arquivos parquet e chamar get_data_for_upload para cada um.
        """
        for parquet_file in self.list_of_parquets:
            self.get_data_for_upload(parquet_file)

if __name__ == "__main__":
    # Instancia a classe Extract e chama o método write_data_to_processed_layer
    extract: Extract = Extract()
    extract.write_data_to_processed_layer()