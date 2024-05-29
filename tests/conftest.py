from pyspark.sql import SparkSession
from _pytest.fixtures import fixture


DATE_AS_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
DATE_AS_STRING_FORMAT_WITH_MS = "%Y-%m-%dT%H:%M:%S.%f%z"

@fixture(scope="function")
def pyspark_session():
    pyspark_session = SparkSession.builder.appName('test').getOrCreate()
    yield pyspark_session
    pyspark_session.stop()

