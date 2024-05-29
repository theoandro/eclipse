import pytest
from src.refining.commonSchemas import RAW_ENERGY_TRADE_SCHEMA
from src.refining.tasks.averageFinancialProductsTask.schema import AVERAGE_FINANCIAL_PRODUCT_SCHEMA
from src.refining.tasks.averageFinancialProductsTask.average_financial_product_task import AverageFinancialProductTask
from pandas import testing
from datetime import datetime
from tests.conftest import DATE_AS_STRING_FORMAT

@pytest.mark.unit
def test_compute_average_trade_price_should_compute_one_average_for_one_financial_product(pyspark_session):
    # Given
    average_financial_product_task = AverageFinancialProductTask(pyspark_session)
    input_row_data = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0,
            40.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -5.0,
            5.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0,
            3.0
        ),
    ]
    input_df = pyspark_session.createDataFrame(input_row_data, RAW_ENERGY_TRADE_SCHEMA)

    # When
    actual_df = average_financial_product_task.process_data(input_df)

    # Then
    expected_row = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        10.0,
        16.0
    )]
    expected_df = pyspark_session.createDataFrame(expected_row, AVERAGE_FINANCIAL_PRODUCT_SCHEMA)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)

@pytest.mark.unit
def test_compute_average_trade_price_should_compute_two_average_for_two_financial_product(pyspark_session):
    # Given
    average_financial_product_task = AverageFinancialProductTask(pyspark_session)

    input_row_data = [
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            20.0,
            40.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            -10.0,
            10.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            15.0,
            3.0
        ),
        (
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            "FR",
            5.0,
            2.0
        ),
    ]
    input_df = pyspark_session.createDataFrame(input_row_data, RAW_ENERGY_TRADE_SCHEMA)

    # When
    actual_df = average_financial_product_task.process_data(input_df)

    # Then
    expected_row = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        5.0,
        25.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        10.0,
        2.5
    )]
    expected_df = pyspark_session.createDataFrame(expected_row, AVERAGE_FINANCIAL_PRODUCT_SCHEMA)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)
