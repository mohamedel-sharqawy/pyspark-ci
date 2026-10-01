from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_job import clean_data


def create_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTest")
        .getOrCreate()
    )


def create_test_dataframe(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    data = [
        ("Mohamed", 100.0),
        ("Ahmed", 200.0),
        ("Omar", 0.0),
        ("Ali", -50.0),
        (None, 300.0),
    ]

    return spark.createDataFrame(data, schema)


def test_valid_records_are_kept():
    spark = create_spark()
    df = create_test_dataframe(spark)

    result = clean_data(df)

    names = {row["name"] for row in result.collect()}

    assert names == {"Mohamed", "Ahmed"}

    spark.stop()


def test_records_with_non_positive_amount_are_removed():
    spark = create_spark()
    df = create_test_dataframe(spark)

    result = clean_data(df)

    amounts = [row["amount"] for row in result.collect()]

    assert 0.0 not in amounts
    assert -50.0 not in amounts

    spark.stop()


def test_records_with_null_names_are_removed():
    spark = create_spark()
    df = create_test_dataframe(spark)

    result = clean_data(df)

    names = [row["name"] for row in result.collect()]

    assert None not in names

    spark.stop()


def test_amount_with_tax_is_calculated_correctly():
    spark = create_spark()
    df = create_test_dataframe(spark)

    result = clean_data(df)

    result_dict = {
        row["name"]: row["amount_with_tax"]
        for row in result.collect()
    }

    assert result_dict["Mohamed"] == 120.0
    assert result_dict["Ahmed"] == 240.0

    spark.stop()