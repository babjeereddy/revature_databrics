import dlt
from pyspark.sql import functions as F

RAW_CUSTOMERS = "project.bronze.customers_raw"
RAW_ORDERS    = "project.bronze.orders_raw"

@dlt.table(name="customers_clean",
           comment="Cleaned customers raw_data")
           
def customers():
    return (
        spark.readStream.table(RAW_CUSTOMERS).select("customer_id","customer_name",
         "email", "city", "state", "country")
            .filter("customer_id IS NOT NULL")
    )

@dlt.table(name="orders_clean")
def orders_clean():
    return (
        spark.readStream.table(RAW_ORDERS)
            .filter("order_id IS NOT NULL")
    )

@dlt.table(name="orders_enriched")
def orders_enriched():
    o = dlt.read("orders_clean")
    c = dlt.read("customers_clean")
    return o.join(c, "customer_id", "left")

@dlt.view(name="orders_view")
def orders_view():
    return (
        dlt.read("orders_enriched")
          .select("order_id", "customer_id", "status")  # add "quantity" if it exists
    )
