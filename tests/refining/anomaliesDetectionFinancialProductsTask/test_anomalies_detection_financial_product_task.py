import pytest
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pandas import testing
from datetime import datetime
from tests.conftest import DATE_AS_STRING_FORMAT
from src.refining.tasks.anomaliesDetectionFinancialProductsTask.anomalies_detection_financial_product_task import AnomaliesDetectionFinancialProductsTask
from src.refining.column_names import PRODUCT_COL, DELIVERY_START_COL, DELIVERY_END_COL, DELIVERY_AREA_COL, PRICE_COL, AVERAGE_TRADE_PRICE_COL

@pytest.mark.unit
def test_compute_average_trade_price_column_should_create_average_price_column_with_one_financial_product(pyspark_session):
    # Given
    anomalies_detection_financial_product_task = AnomaliesDetectionFinancialProductsTask(pyspark_session)
    input_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False)
    ])

    input_row_data = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -5.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0
        ),
    ]
    input_df = pyspark_session.createDataFrame(input_row_data, input_schema)

    # When
    actual_df = anomalies_detection_financial_product_task.compute_average_trade_price_column(input_df)

    # Then
    output_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False),
        StructField(AVERAGE_TRADE_PRICE_COL, FloatType(), False)
    ])
    expected_row = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0,
            10.0,
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -5.0,
            10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0,
            10.0
        ),
    ]

    expected_df = pyspark_session.createDataFrame(expected_row, output_schema)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)

@pytest.mark.unit
def test_compute_average_trade_price_column_should_create_average_price_column_with_two_financial_products(pyspark_session):
    # Given
    anomalies_detection_financial_product_task = AnomaliesDetectionFinancialProductsTask(pyspark_session)
    input_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False)
    ])

    input_row_data = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            5.0
        ),
    ]
    input_df = pyspark_session.createDataFrame(input_row_data, input_schema)

    # When
    actual_df = anomalies_detection_financial_product_task.compute_average_trade_price_column(input_df)

    # Then
    output_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False),
        StructField(AVERAGE_TRADE_PRICE_COL, FloatType(), False)
    ])
    expected_row = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0,
            10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            5.0,
            10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0,
            5.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -10.0,
            5.0
        )
    ]

    expected_df = pyspark_session.createDataFrame(expected_row, output_schema)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)

@pytest.mark.unit
def test_compute_anomalies_from_trade_price_should_filter_anoamlies_from_raw_data(pyspark_session):
    # Given
    anomalies_detection_financial_product_task = AnomaliesDetectionFinancialProductsTask(pyspark_session)
    input_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False),
        StructField(AVERAGE_TRADE_PRICE_COL, FloatType(), False)
    ])

    input_row_data = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            35.0,
            10.0,
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            5.0,
            10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0,
            10.0
        ),
    ]
    input_df = pyspark_session.createDataFrame(input_row_data, input_schema)

    # When
    actual_df = anomalies_detection_financial_product_task.compute_anomalies_from_trade_price(input_df)

    # Then
    output_schema = StructType([
        StructField(PRODUCT_COL, StringType(), False),
        StructField(DELIVERY_START_COL, TimestampType(), False),
        StructField(DELIVERY_END_COL, TimestampType(), False),
        StructField(DELIVERY_AREA_COL, StringType(), False),
        StructField(PRICE_COL, FloatType(), False),
        StructField(AVERAGE_TRADE_PRICE_COL, FloatType(), False)
    ])
    expected_row = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            35.0,
            10.0,
        )
    ]

    expected_df = pyspark_session.createDataFrame(expected_row, output_schema)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)
