import pytest
from src.data_exploration import data_exploration
from pyspark.sql import SparkSession


@pytest.mark.mandatory
def test_data_exploration():
    spark = SparkSession.builder.master("local[1]").appName('project1').getOrCreate()
    file_path = "C:\\Users\\91790\\PycharmProjects\\project1-main\\dataset\\nyc-jobs.csv"
    df = spark.read.option("multiline", "true").option("quote", '"').option("header", "true").option("escape", "\\").option("escape", '"').csv(file_path, header=True)
    output = data_exploration(df)
    assert output == "Success"
