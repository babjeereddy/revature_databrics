from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FileSinkExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Source: streaming text files
df = spark.readStream.format("text").load("C:/data/input1")

# Sink: write as TEXT files
query = df.writeStream \
    .format("text") \
    .outputMode("append") \
    .option("path", "C:/data/output/text_sink") \
    .option("checkpointLocation", "C:/data/checkpoints/text_cp") \
    .start()

query.awaitTermination()
