from click import command, option
import os
from ingestion.ingestion_task import IngestionTask
from ingestion.task_manager import task_manager


@command("ingestion-task")
@option(
    "--filename-input",
    type=str,
    default="./data/Continuous_Trades-FR-20230101-20230102T001700000Z.csv",
    help="Name of the csv input.",
)
@option(
    "--filename-output",
    type=str,
    default="ingestion_output",
    help="Name of the parquet output.",
)
def main(
        filename_input: str,
        filename_output: str,
):
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_container_url = os.getenv("AWS_CONTAINER_URL")
    with task_manager(aws_access_key_id, aws_secret_access_key) as pyspark_session:
        output_path = f"{aws_container_url}/bronze/{filename_output}"
        task = IngestionTask(pyspark_session)
        task.launch(filename_input, output_path)


if __name__ == "__main__":
    main()