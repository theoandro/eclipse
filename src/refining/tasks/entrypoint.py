from click import Choice, argument, command, option
import os
from refining.tasks.highestVolumeFinancialProductsTask.highest_volume_financial_product_task import HighestVolumeFinancialProductTask
from refining.tasks.averageFinancialProductsTask.average_financial_product_task import AverageFinancialProductTask
from refining.tasks.anomaliesDetectionFinancialProductsTask.anomalies_detection_financial_product_task import AnomaliesDetectionFinancialProductsTask
from refining.tasks.task_manager import task_manager
TASK_NAME_TO_TASK_CLASS_NAME_AND_OUTPUT_FILE_PATH_MAP = {
    "average_financial_product_task":
        {"class": AverageFinancialProductTask, "output_file_path": "average_finicial_product"},
    "highest_volume_financial_product_task":
        {"class": HighestVolumeFinancialProductTask, "output_file_path": "highest_volume_financial_product"},
    "anomalies_detection_financial_product_task":
        {"class": AnomaliesDetectionFinancialProductsTask, "output_file_path": "anomalies_detection_financial_product"}
}


@command("refining-task")
@argument("task", type=Choice(["average_financial_product_task", "highest_volume_financial_product_task", "anomalies_detection_financial_product_task"]))
@option(
    "--filename-input",
    type=str,
    default="ingestion_output",
    help="Name of the csv input.",
)
def main(
        task: str,
        filename_input: str,
):
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_container_url = os.getenv("AWS_CONTAINER_URL")
    task_class = TASK_NAME_TO_TASK_CLASS_NAME_AND_OUTPUT_FILE_PATH_MAP[task]["class"]
    output_file_path = TASK_NAME_TO_TASK_CLASS_NAME_AND_OUTPUT_FILE_PATH_MAP[task]["output_file_path"]
    input_path = f"{aws_container_url}/bronze/{filename_input}"
    output_path = f"{aws_container_url}/gold/{output_file_path}"
    with task_manager(aws_access_key_id, aws_secret_access_key) as pyspark_session:
        task = task_class(pyspark_session)
        task.launch(input_path, output_path)


if __name__ == "__main__":
    main()
