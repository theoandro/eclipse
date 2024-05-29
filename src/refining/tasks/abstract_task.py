from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from refining.tasks.schema import INGESTION_OUTPUT_SCHEMA

PATH_TO_DATA = "/Users/theo.andro/Desktop/Eclipse/data_engineer_assignment/data"


class AbstractTask(ABC):
    pyspark_session: SparkSession

    def read_raw_data(self, input_file_path):
        return self.pyspark_session.read.schema(INGESTION_OUTPUT_SCHEMA).parquet(input_file_path).filter(col("Side")=="SELL")


    @abstractmethod
    def process_data(self, input_data: DataFrame) -> DataFrame:
        raise NotImplementedError("Implement me!")

    def write_data(self, processed_data: DataFrame, output_file_path: str):
        processed_data.write.mode('overwrite').parquet(output_file_path)

    def launch(self, input_file_path, output_file_path):
        input_raw_data = self.read_raw_data(input_file_path)
        processed_data = self.process_data(input_raw_data)
        self.write_data(processed_data, output_file_path)
