from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import avg, col, abs
from refining.tasks.abstract_task import AbstractTask
from refining.tasks.anomaliesDetectionFinancialProductsTask.schema import ANOMALIES_DETECTION_FINANCIAL_PRODUCT_SCHEMA
from refining.column_names import (
    PRODUCT_COL,
    DELIVERY_START_COL,
    DELIVERY_END_COL,
    DELIVERY_AREA_COL,
    PRICE_COL,
    AVERAGE_TRADE_PRICE_COL)

class AnomaliesDetectionFinancialProductsTask(AbstractTask):
    def __init__(self, pyspark_session: SparkSession):
        self.pyspark_session = pyspark_session

    def process_data(self, input_data: DataFrame) -> DataFrame:
        input_data_with_average_price_column = self.compute_average_trade_price_column(input_data)
        return self.compute_anomalies_from_trade_price(input_data_with_average_price_column) \
            .select(ANOMALIES_DETECTION_FINANCIAL_PRODUCT_SCHEMA.fieldNames())

    @staticmethod
    def compute_average_trade_price_column(raw_energy_trade_df: DataFrame) -> DataFrame:
        return raw_energy_trade_df \
            .withColumn(AVERAGE_TRADE_PRICE_COL, avg(PRICE_COL) \
            .over(Window.partitionBy(PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL)))

    @staticmethod
    def compute_anomalies_from_trade_price(raw_energy_trade_with_average_trade_price_df: DataFrame) -> DataFrame:
        return raw_energy_trade_with_average_trade_price_df \
            .filter(abs(col(AVERAGE_TRADE_PRICE_COL) - col(PRICE_COL)) > col(AVERAGE_TRADE_PRICE_COL) * 2)

