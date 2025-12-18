CREATE OR REFRESH STREAMING LIVE TABLE orders_stream
AS
SELECT
  order_id,
  customer_id,
  status,
  ingestion_ts
FROM STREAM(project.bronze.orders_raw)
WHERE order_id IS NOT NULL;


CREATE OR REFRESH STREAMING LIVE TABLE customers_stream
AS
SELECT
  customer_id,
  customer_name,
  city,
  segment,
  ingestion_ts AS customer_ingestion_ts
FROM STREAM(project.bronze.customers_raw)
WHERE customer_id IS NOT NULL;


CREATE OR REFRESH LIVE TABLE orders_enriched
AS
SELECT
  o.order_id,
  o.customer_id,
  o.status,
  o.ingestion_ts AS order_ingestion_ts,
  c.customer_name,
  c.city,
  c.segment,
  c.customer_ingestion_ts
FROM LIVE.orders_stream o
LEFT JOIN LIVE.customers_stream c
  ON o.customer_id = c.customer_id;


CREATE OR REFRESH LIVE VIEW orders_view_sql
AS
SELECT
  order_id,
  customer_name,
  status
FROM LIVE.orders_enriched;
