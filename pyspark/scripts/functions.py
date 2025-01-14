from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as F

import os


def create_session(app_name):
    """Create preconfigured spark session

    Args:
        app_name str: Spark application Name

    Returns:
        SparkSession: spark session
    """
    minio_host = os.getenv("MINIO_HOSTNAME", "minio")
    host_url = f"http://{minio_host}:9000/"

    packages = [
        "com.amazonaws:aws-java-sdk-bundle:1.12.728",
        "org.apache.hadoop:hadoop-aws:3.3.2",
        "io.delta:delta-spark_2.13:3.2.0",
        "org.apache.spark:spark-catalyst_2.13:3.3.2",
    ]

    conf = SparkConf()
    conf.set("spark.jars.packages", ",".join(packages))
    conf.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.shuffle.partitions", "4")
    conf.set("spark.hadoop.fs.s3a.endpoint", host_url)
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(conf=conf)
        .getOrCreate()
    )


def explode_list(df, column_name):
    """
    Explode list column to multiple rows
    """
    temp_df = df.withColumn(column_name, F.explode(column_name))
    field = temp_df.schema[column_name]

    is_struct = isinstance(field.dataType, StructType)
    is_array = isinstance(field.dataType, ArrayType)

    if is_struct:
        temp_df = explode_struct(temp_df, field.name)
    elif is_array:
        temp_df = explode_list(temp_df, field.name)

    return temp_df


def explode_struct(df, column_name):
    """
    Explode struct to multiple columns
    """
    fields = df.schema[column_name].dataType.fields
    temp_df = df.select("*", f"{column_name}.*").drop(column_name)
    for field in fields:
        f_name = field.name
        is_struct = isinstance(field.dataType, StructType)
        is_array = isinstance(field.dataType, ArrayType)

        if "TRUE" == os.getenv("UNDERSCORE_ENABLED"):
            f_name = f"{column_name}_{field.name}"
            temp_df = temp_df.withColumnRenamed(field.name, f_name)

        if is_struct:
            temp_df = explode_struct(temp_df, f_name)
        if is_array:
            temp_df = explode_list(temp_df, f_name)

    return temp_df


def flatten_dataframe(df):
    """
    Normalize dataframes that contains list and structs as columns
    """
    for col in df.schema:
        is_struct = isinstance(col.dataType, StructType)
        is_array = isinstance(col.dataType, ArrayType)

        if is_struct:
            temp_df = explode_struct(df, col.name)
        elif is_array:
            temp_df = explode_list(df, col.name)
    return temp_df
