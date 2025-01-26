from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

def transform_data(input_path, output_path):
    spark = SparkSession.builder.appName("Task2").getOrCreate()

    df = spark.read.parquet(input_path)

    df_beef = df.filter(col("ingredients").contains("beef"))

    df_beef = df_beef.withColumn("total_cook_time", col("cookTime") + col("prepTime")) \
                     .withColumn(
                         "difficulty",
                         when(col("total_cook_time") < 30, "easy")
                         .when((col("total_cook_time") >= 30) & (col("total_cook_time") <= 60), "medium")
                         .otherwise("hard")
                     )

    df_result = df_beef.groupBy("difficulty").agg(avg("total_cook_time").alias("avg_total_cooking_time"))

    df_result.write.mode("overwrite").csv(output_path, header=True)
    
    print(f"Data successfully written to {output_path}")

if __name__ == "__main__":
    input_folder = "C:/Downloads/Assessment - Kalyan/zayyanm2425-HFData-Engineering-Test/input"  
    output_path = "C:/Downloads/Assessment - Kalyan/zayyanm2425-HFData-Engineering-Test/output" 
    transform_data(input_path, output_path)
