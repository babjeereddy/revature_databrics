from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("FileStreamAggExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1) Define schema of incoming CSV files
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", DoubleType(), True)
])

# 2) Folder to watch (create this folder)
input_path = "C:/data/stream_agg"   # adjust if needed

# 3) Read streaming data from CSV files
df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)


deptAgg = df.groupBy("dept").agg(
    _sum("salary").alias("total_salary")
)


query = deptAgg.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
