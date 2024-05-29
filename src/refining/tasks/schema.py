from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, LongType, FloatType
from refining.column_names import (
    TRADE_ID_COL,
    REMOTE_TRADE_ID_COL,
    SIDE_COL,
    PRODUCT_COL,
    DELIVERY_START_COL,
    DELIVERY_END_COL,
    DELIVERY_AREA_COL,
    EXECUTION_TIME_COL,
    TRADE_PHASE_COL,
    USER_DEFINED_BLOCK_COL,
    SELF_TRADE_COL,
    CURRENCY_COL,
    PRICE_COL,
    VOLUME_COL,
    VOLUME_UNIT_COL,
    ORDER_ID_COL)

INGESTION_OUTPUT_SCHEMA = StructType(
    [
        StructField(TRADE_ID_COL, IntegerType(), False),
        StructField(REMOTE_TRADE_ID_COL, IntegerType(), False),
        StructField(SIDE_COL, StringType(), False),
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(EXECUTION_TIME_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(TRADE_PHASE_COL, StringType(), False),
        StructField(USER_DEFINED_BLOCK_COL, StringType(), False),
        StructField(SELF_TRADE_COL, StringType(), False),
        StructField(PRICE_COL, DoubleType(), False),
        StructField(CURRENCY_COL, StringType(), False),
        StructField(VOLUME_COL, FloatType(), False),
        StructField(VOLUME_UNIT_COL, StringType(), False),
        StructField(ORDER_ID_COL, LongType(), False)
    ])



