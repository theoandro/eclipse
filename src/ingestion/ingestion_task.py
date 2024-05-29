from pyspark.sql import SparkSession, DataFrame
from ingestion.schemas import ENERGY_TRADES_CSV_SCHEMA

class IngestionTask:
    def __init__(self, pyspark_session: SparkSession):
        self.pyspark_session = pyspark_session

    def read_energy_trades_from_csv(self, file_path: str) -> DataFrame:
        return self.pyspark_session \
            .read \
            .option("delimiter", ",") \
            .option("header", True) \
            .schema(ENERGY_TRADES_CSV_SCHEMA) \
            .csv(file_path)

    # def read_energy_trades_from_web_socket(self) -> DataFrame:
    #     return self.pyspark_session \
    #             .readStream \
    #             .format("socket") \
    #             .option("host", "localhost") \
    #             .option("port", 8765) \
    #             .load()

    @staticmethod
    def write_ingestion_result(ingestion_df: DataFrame, file_path: str):
        ingestion_df.write.mode("overwrite").parquet(file_path)

    # @staticmethod
    # def write_ingestion_stream_result(ingestion_df: DataFrame, file_path: str):
    #     query = ingestion_df \
    #         .writeStream \
    #         .outputMode("append") \
    #         .format("console") \
    #         .option("truncate", False) \
    #         .option("numRows", 1000) \
    #         .start()
    #     print("lines isStreaming: ", ingestion_df.isStreaming)

    def launch(self, input_path, output_path):
        raw_data = self.read_energy_trades_from_csv(input_path)
        self.write_ingestion_result(raw_data, output_path)
