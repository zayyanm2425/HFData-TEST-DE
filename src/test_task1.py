import pytest
from pyspark.sql import SparkSession
from task1 import preprocess_data

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestTask1").getOrCreate()

def test_preprocess_data(spark, tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    
    
    input_data = [{"name": "Recipe1", "cookTime": "PT10M", "prepTime": "PT5M", "ingredients": "beef, salt"}]
    spark.createDataFrame(input_data).write.json(str(input_path))
    
    
    preprocess_data(str(input_path), str(output_path))
    
    
    df = spark.read.parquet(str(output_path))
    assert df.count() == 1
