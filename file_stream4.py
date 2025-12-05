from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("CSVtoJSONSink") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema of incoming CSV
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("dept", StringType(), True)
])

input_path = "C:/data/input_csv_stream"   # folder to DROP CSV files

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)

high_salary_df = df.filter(df.salary > 50000)

query = high_salary_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "C:/data/output/json_sink") \
    .option("checkpointLocation", "C:/data/checkpoints/json_cp") \
    .start()

query.awaitTermination()
