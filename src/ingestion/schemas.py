from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, LongType, FloatType

ENERGY_TRADES_CSV_SCHEMA = StructType(
    [
        StructField("TradeId", IntegerType(), False),
        StructField("RemoteTradeId", IntegerType(), False),
        StructField("Side", StringType(), False),
        StructField("Product", StringType(), False),
        StructField("DeliveryStart", TimestampType(), False),
        StructField("DeliveryEnd", TimestampType(), False),
        StructField("ExecutionTime", TimestampType(), False),
        StructField("DeliveryArea", StringType(), False),
        StructField("TradePhase", StringType(), False),
        StructField("UserDefinedBlock", StringType(), False),
        StructField("SelfTrade", StringType(), False),
        StructField("Price", DoubleType(), False),
        StructField("Currency", StringType(), False),
        StructField("Volume", FloatType(), False),
        StructField("VolumeUnit", StringType(), False),
        StructField("OrderID", LongType(), False)
        ])



