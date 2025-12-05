from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SocketStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read text stream
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Word count
words = lines.selectExpr("explode(split(value, ' ')) as word")
wordCount = words.groupBy("word").count()

query = wordCount.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()