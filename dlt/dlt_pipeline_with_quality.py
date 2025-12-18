import dlt
from pyspark.sql import functions as F

ORDERS_PATH = "dbfs:/FileStore/dlt_demo/orders"
CUSTOMERS_PATH = "dbfs:/FileStore/dlt_demo/customers"

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested using Auto Loader"
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(ORDERS_PATH)
            .withColumn("ingest_ts", F.current_timestamp())
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customers ingested using Auto Loader"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(CUSTOMERS_PATH)
            .withColumn("ingest_ts", F.current_timestamp())
    )

@dlt.table(
    name="silver_orders",
    comment="Validated and cleaned orders"
)
@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("amount_positive", "amount > 0")
@dlt.expect_or_drop("valid_status", "status IN ('NEW','SHIPPED','CANCELLED')")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .withColumn("order_ts", F.to_timestamp("order_ts"))
            .select(
                "order_id",
                "order_ts",
                "customer_id",
                "amount",
                "status"
            )
    )


@dlt.table(
    name="silver_customers",
    comment="Cleaned customer master data"
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
            .select(
                "customer_id",
                "customer_name",
                "city",
                "segment"
            )
    )

# GOLD – JOINED BUSINESS TABLE
@dlt.table(
    name="gold_orders_enriched",
    comment="Orders enriched with customer details"
)
def gold_orders_enriched():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")

    return (
        orders.join(customers, "customer_id", "left")
    )

# GOLD – DAILY SALES SUMMARY
@dlt.table(
    name="gold_daily_sales"
)
def gold_daily_sales():
    df = dlt.read("gold_orders_enriched")

    return (
        df.groupBy(
            F.to_date("order_ts").alias("order_date"),
            "city",
            "segment"
        )
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("amount").alias("total_sales")
        )
    )
