from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def preprocess_data(input_folder, output_path):
    spark = SparkSession.builder.appName("Task1").getOrCreate()
   
    df = spark.read.json(f"{input_folder}/*.json")
    df_processed = df.dropDuplicates().na.fill({"cookTime": "PT0M", "prepTime": "PT0M", "ingredients": ""})

    df_processed.write.mode("overwrite").parquet(output_path)
    
    print(f"Data successfully written to {output_path}")

if __name__ == "__main__":
    input_folder = "C:/Downloads/Assessment - Kalyan/zayyanm2425-HFData-Engineering-Test/input"  
    output_path = "C:/Downloads/Assessment - Kalyan/zayyanm2425-HFData-Engineering-Test/output" 
    preprocess_data(input_folder, output_path)
