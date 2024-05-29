from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from refining.column_names import PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL, PRICE_COL, VOLUME_COL


RAW_ENERGY_TRADE_SCHEMA = StructType([
    StructField(PRODUCT_COL, StringType(), False),
    StructField(DELIVERY_START_COL, TimestampType(), False),
    StructField(DELIVERY_END_COL, TimestampType(), False),
    StructField(DELIVERY_AREA_COL, StringType(), False),
    StructField(PRICE_COL, FloatType(), False),
    StructField(VOLUME_COL, FloatType(), False)
])
