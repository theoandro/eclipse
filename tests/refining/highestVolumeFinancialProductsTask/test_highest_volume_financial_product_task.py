import pytest
from src.refining.commonSchemas import RAW_ENERGY_TRADE_SCHEMA
from src.refining.tasks.highestVolumeFinancialProductsTask.schema import HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA
from src.refining.tasks.highestVolumeFinancialProductsTask.highest_volume_financial_product_task import HighestVolumeFinancialProductTask
from pandas import testing
from datetime import datetime
from tests.conftest import DATE_AS_STRING_FORMAT


@pytest.mark.unit
def test_compute_volume_sum_by_financial_product_should_compute_one_sum_for_one_financial_product(pyspark_session):
    # Given
    highest_volume_financial_product_task = HighestVolumeFinancialProductTask(pyspark_session)
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
    actual_df = highest_volume_financial_product_task.compute_volume_sum_by_financial_product(input_df)

    # Then
    expected_row = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        48.0,
    )]
    expected_df = pyspark_session.createDataFrame(expected_row, HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)


@pytest.mark.unit
def test_compute_volume_sum_by_financial_product_should_compute_two_sum_for_two_financial_product(pyspark_session):
    # Given
    highest_volume_financial_product_task = HighestVolumeFinancialProductTask(pyspark_session)
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
    actual_df = highest_volume_financial_product_task.compute_volume_sum_by_financial_product(input_df)

    # Then
    expected_row = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        50.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        5.0
    )]
    expected_df = pyspark_session.createDataFrame(expected_row, HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)


@pytest.mark.unit
def test_compute_n_highest_volume_financial_product_should_compute_df_with_5_top_volume(pyspark_session):
    # Given
    highest_volume_financial_product_task = HighestVolumeFinancialProductTask(pyspark_session)

    nb_rows_to_select = 5
    input_row_data = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        50.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        40.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        30.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        20.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        10.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        5.0
    )]

    input_df = pyspark_session.createDataFrame(input_row_data, HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA)

    # When
    actual_df = highest_volume_financial_product_task.compute_n_highest_volume_financial_product(input_df, nb_rows_to_select)

    # Then
    expected_rows = [(
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        50.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        40.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        30.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        20.0
    ), (
        "XBID_Hour_Power",
        datetime.strptime("2023-01-01T09:00:00Z", DATE_AS_STRING_FORMAT),
        datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
        "FR",
        10.0
    )]

    expected_df = pyspark_session.createDataFrame(expected_rows, HIGHEST_VOLUME_FINANCIAL_PRODUCT_SCHEMA)
    testing.assert_frame_equal(actual_df.toPandas(), expected_df.toPandas(), check_dtype=False)

