import pytest
from datetime import datetime
from pandas import testing
from src.ingestion.schemas import ENERGY_TRADES_CSV_SCHEMA
from src.ingestion.ingestion_task import IngestionTask
from tests.conftest import DATE_AS_STRING_FORMAT, DATE_AS_STRING_FORMAT_WITH_MS


@pytest.mark.unit
def test_read_energy_trades_from_csv_should_read_correctly_a_csv_sample_energy_trades(
        pyspark_session
):
    # Given
    ingestion_task = IngestionTask(pyspark_session)
    sample_json_path = "/Users/theo.andro/Desktop/Eclipse/data_engineer_assignment/tests/ingestion/sample.csv"


    # When
    actual_energy_trades_df = ingestion_task.read_energy_trades_from_csv(sample_json_path)

    # Then
    expected_rows = [
        (
            1363439822,
            221153888,
            "SELL",
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2022-12-31T16:05:06.392Z", DATE_AS_STRING_FORMAT_WITH_MS),
            "FR",
            "CONT",
            "N",
            "U",
            -5.03,
            "EUR",
            5.0,
            "MWH",
            13550086602
        ),
        (
            1363439823,
            221153889,
            "SELL",
            "XBID_Hour_Power",
            datetime.strptime("2023-01-01T10:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2023-01-01T11:00:00Z", DATE_AS_STRING_FORMAT),
            datetime.strptime("2022-12-31T16:05:06.392Z", DATE_AS_STRING_FORMAT_WITH_MS),
            "FR",
            "CONT",
            "N",
            "U",
            -5.03,
            "EUR",
            0.8,
            "MWH",
            13550086602
        )
    ]
    expected_df = pyspark_session.createDataFrame(expected_rows, ENERGY_TRADES_CSV_SCHEMA)

    testing.assert_frame_equal(actual_energy_trades_df.toPandas(), expected_df.toPandas(), check_dtype=False)


@pytest.mark.integration
def test_read_energy_trades_from_entire_csv_should_build_a_df_with_all_rows(
        pyspark_session
):
    # Given
    ingestion_task = IngestionTask(pyspark_session)
    sample_json_path = "/Users/theo.andro/Desktop/Eclipse/data_engineer_assignment/data/Continuous_Trades-FR-20230101-20230102T001700000Z.csv"

    # When
    actual_energy_trades_df = ingestion_task.read_energy_trades_from_csv(sample_json_path)

    # Then
    nb_line_in_historical_trade_csv = 32731
    pandas_df = actual_energy_trades_df.toPandas()
    assert len(pandas_df) == nb_line_in_historical_trade_csv
