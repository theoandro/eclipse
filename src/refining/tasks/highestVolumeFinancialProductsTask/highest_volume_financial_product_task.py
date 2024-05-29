from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum
from refining.tasks.abstract_task import AbstractTask
from refining.tasks.highestVolumeFinancialProductsTask.schema import HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA
from refining.column_names import (
    PRODUCT_COL,
    DELIVERY_START_COL,
    DELIVERY_END_COL,
    DELIVERY_AREA_COL,
    VOLUME_COL,
)
TOP_VALUES_TO_SELECT = 5


class HighestVolumeFinancialProductTask(AbstractTask):
    def __init__(self, pyspark_session: SparkSession):
        self.pyspark_session = pyspark_session

    def process_data(self, input_data: DataFrame) -> DataFrame:
        financial_product_with_volume_sum_df = self.compute_volume_sum_by_financial_product(input_data)
        return self.compute_n_highest_volume_financial_product(financial_product_with_volume_sum_df, TOP_VALUES_TO_SELECT).select(HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA.fieldNames())

    @staticmethod
    def compute_volume_sum_by_financial_product(raw_energy_trade_df: DataFrame) -> DataFrame:
        return raw_energy_trade_df \
            .groupby(PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL) \
            .agg(sum(VOLUME_COL).alias(VOLUME_COL))

    @staticmethod
    def compute_n_highest_volume_financial_product(financial_product_with_volume_df: DataFrame, top_values_to_select: int) -> DataFrame:
        return financial_product_with_volume_df \
            .sort(VOLUME_COL, ascending=False) \
            .limit(top_values_to_select)
