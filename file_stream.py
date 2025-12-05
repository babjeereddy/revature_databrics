from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("FileStreamExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema for CSV
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])

input_path = "C:/data/stream_input_data"   

# Read Streaming Files from a Folder
df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)

high_salary_df = df.groupBy().agg(sum(col("salary")).alias("total_sal"))

query = high_salary_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
