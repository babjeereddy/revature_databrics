from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("CSVtoCSVSink") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])


input_path = "C:/data/input_csv_stream"

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)

query = df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "C:/data/output") \
    .option("checkpointLocation", "C:/data/output/csv_checkpoint") \
    .option("header", "true") \
    .start()

query.awaitTermination()
