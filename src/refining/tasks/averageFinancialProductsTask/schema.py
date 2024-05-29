from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from refining.column_names import PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL, AVERAGE_TRADE_PRICE_COL, AVERAGE_VOLUME_COL

AVERAGE_FINANCIAL_PRODUCT_SCHEMA = StructType([
    StructField(PRODUCT_COL, StringType(), False),
    StructField(DELIVERY_START_COL, TimestampType(), False),
    StructField(DELIVERY_END_COL, TimestampType(), False),
    StructField(DELIVERY_AREA_COL, StringType(), False),
    StructField(AVERAGE_TRADE_PRICE_COL, FloatType(), False),
    StructField(AVERAGE_VOLUME_COL, FloatType(), False)
])