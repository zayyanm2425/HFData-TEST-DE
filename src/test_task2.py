import pytest
from pyspark.sql import SparkSession
from task2 import transform_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestTask2").getOrCreate()

def test_transform_data(spark, tmp_path):
    input_path = tmp_path / "processed"
    output_path = tmp_path / "results"
    
    
    input_data = [
        {"name": "Recipe1", "cookTime": 20, "prepTime": 10, "ingredients": "beef, salt"},
        {"name": "Recipe2", "cookTime": 40, "prepTime": 20, "ingredients": "chicken, salt"}
    ]
    spark.createDataFrame(input_data).write.parquet(str(input_path))
    
    
    transform_data(str(input_path), str(output_path))
    
    
    df = spark.read.csv(str(output_path), header=True)
    assert df.count() == 2
    assert "difficulty" in df.columns
    assert "avg_total_cooking_time" in df.columns
