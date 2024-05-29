from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg
from refining.tasks.abstract_task import AbstractTask
from refining.tasks.averageFinancialProductsTask.schema import AVERAGE_FINANCIAL_PRODUCT_SCHEMA
from refining.column_names import (
    PRODUCT_COL,
    DELIVERY_START_COL,
    DELIVERY_END_COL,
    DELIVERY_AREA_COL,
    PRICE_COL,
    VOLUME_COL,
    AVERAGE_TRADE_PRICE_COL,
    AVERAGE_VOLUME_COL)

class AverageFinancialProductTask(AbstractTask):
    def __init__(self, pyspark_session: SparkSession):
        self.pyspark_session = pyspark_session

    def process_data(self, input_data: DataFrame) -> DataFrame:
        return self.compute_average_trade_price(input_data)

    @staticmethod
    def compute_average_trade_price(raw_energy_trade_df: DataFrame) -> DataFrame:
        return raw_energy_trade_df \
            .groupby(PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL) \
            .agg(
            avg(PRICE_COL).alias(AVERAGE_TRADE_PRICE_COL),
            avg(VOLUME_COL).alias(AVERAGE_VOLUME_COL)
        ).select(AVERAGE_FINANCIAL_PRODUCT_SCHEMA.fieldNames())

