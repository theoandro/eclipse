from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

@contextmanager
def task_manager(aws_access_key_id: str, aws_secret_access_key: str):
    conf = (
        SparkConf()
        .setAppName("MY_APP") # replace with your desired name
        .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
        .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
        .setMaster("local[*]")
    )
    pyspark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    yield pyspark_session
    pyspark_session.stop()
