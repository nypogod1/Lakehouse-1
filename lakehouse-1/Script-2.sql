CREATE SCHEMA iceberg.test;
CREATE TABLE iceberg.test.t AS SELECT * FROM tpch.tiny.customer;
SELECT count(*) FROM iceberg.test.t;

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

CREATE TABLE iceberg.bronze.customer WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.customer;
CREATE TABLE iceberg.bronze.orders WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.orders;
CREATE TABLE iceberg.bronze.lineitem WITH (format='PARQUET') AS SELECT * FROM tpch.sf1.lineitem;

SELECT count(*) FROM iceberg.bronze.lineitem;

SELECT sum(file_size_in_bytes)/1024/1024 AS mb FROM iceberg.bronze.lineitem."$files";



SELECT sum(file_size_in_bytes)/1024/1024 AS mb
FROM iceberg.bronze."lineitem$files";

SELECT sum(file_size_in_bytes)/1024/1024 AS mb FROM iceberg.bronze.customer."$files";

CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE TABLE iceberg.silver.big_orders WITH (format='PARQUET') AS
SELECT orderkey, sum(quantity) AS total_quantity
FROM iceberg.bronze.lineitem
GROUP BY orderkey
HAVING sum(quantity) > 300;


CREATE TABLE iceberg.silver.big_order_customers WITH (format='PARQUET') AS
SELECT trim(c.name) AS customer_name,
       c.custkey, o.orderkey, o.orderdate,
       CAST(o.totalprice AS DECIMAL(12,2)) AS totalprice,
       b.total_quantity
FROM iceberg.bronze.customer c
JOIN iceberg.bronze.orders  o ON c.custkey = o.custkey
JOIN iceberg.silver.big_orders b ON o.orderkey = b.orderkey
WHERE o.totalprice IS NOT NULL AND c.name IS NOT NULL;


SELECT count(*) FROM iceberg.silver.big_orders;
SELECT sum(file_size_in_bytes)/1024/1024 AS mb FROM iceberg.silver.big_orders."$files";

SELECT count(*) FROM iceberg.silver.big_order_customers;

CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE iceberg.gold.top100_large_volume_customers WITH (format='PARQUET') AS
SELECT customer_name AS name, custkey, orderkey, orderdate, totalprice, total_quantity
FROM iceberg.silver.big_order_customers
ORDER BY totalprice DESC, orderdate
LIMIT 100;

SELECT count(*) FROM iceberg.gold.top100_large_volume_customers;

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM tpch.sf1.customer c
JOIN tpch.sf1.orders o ON c.custkey = o.custkey
JOIN tpch.sf1.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (SELECT orderkey FROM tpch.sf1.lineitem
                     GROUP BY orderkey HAVING sum(quantity) > 300)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;

CREATE TABLE iceberg.bronze_uncompressed.customer
WITH (format = 'PARQUET', compression_codec = 'NONE')
AS
SELECT * FROM tpch.sf1.customer;

CREATE TABLE iceberg.bronze_uncompressed.lineitem
WITH (format = 'PARQUET', compression_codec = 'NONE')
AS SELECT * FROM tpch.sf1.orders;

CREATE SCHEMA IF NOT EXISTS iceberg.bronze_snappy;

CREATE TABLE iceberg.bronze_snappy.lineitem
WITH (format = 'PARQUET', compression_codec = 'SNAPPY')
AS SELECT * FROM tpch.sf1.customer;

CREATE SCHEMA IF NOT EXISTS iceberg.bronze_gzip;

CREATE TABLE iceberg.bronze_gzip.customer
WITH (format = 'PARQUET', compression_codec = 'GZIP')
AS SELECT * FROM tpch.sf1.customer;

CREATE TABLE iceberg.bronze_gzip.lineitem
WITH (format = 'PARQUET', compression_codec = 'GZIP')
AS SELECT * FROM tpch.sf1.orders;
CREATE SCHEMA IF NOT EXISTS iceberg.bronze_zstd;

CREATE TABLE iceberg.bronze_zstd.customer
WITH (format = 'PARQUET', compression_codec = 'ZSTD')
AS SELECT * FROM tpch.sf1.customer;


CREATE TABLE iceberg.bronze_zstd.orders
WITH (format = 'PARQUET', compression_codec = 'ZSTD')
AS SELECT * FROM tpch.sf1.orders;

CREATE TABLE iceberg.bronze_zstd.lineitem
WITH (format = 'PARQUET', compression_codec = 'ZSTD')
AS SELECT * FROM tpch.sf1.lineitem;

SELECT query, elapsed_time
FROM system.runtime.queries
WHERE query LIKE '%CREATE TABLE iceberg.bronze_%'
ORDER BY created DESC
LIMIT 30;

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM iceberg.bronze_none.customer c
JOIN iceberg.bronze_none.orders   o ON c.custkey = o.custkey
JOIN iceberg.bronze_none.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey FROM iceberg.bronze_none.lineitem
  GROUP BY orderkey HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;


SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM iceberg.bronze_gzip.customer c
JOIN iceberg.bronze_gzip.orders   o ON c.custkey = o.custkey
JOIN iceberg.bronze_gzip.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey FROM iceberg.bronze_gzip.lineitem
  GROUP BY orderkey HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;


CREATE SCHEMA IF NOT EXISTS iceberg.bronze_orc;
CREATE TABLE iceberg.bronze_orc.lineitem
WITH (format = 'ORC', compression_codec = 'ZSTD')
AS SELECT * FROM tpch.sf1.lineitem;

SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM iceberg.bronze_orc.customer c
JOIN iceberg.bronze_orc.orders   o ON c.custkey = o.custkey
JOIN iceberg.bronze_orc.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey FROM iceberg.bronze_orc.lineitem
  GROUP BY orderkey HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;

ALTER TABLE iceberg.silver.big_order_customers ADD COLUMN order_year INTEGER;
UPDATE iceberg.silver.big_order_customers SET order_year = year(orderdate);
ALTER TABLE iceberg.silver.big_order_customers RENAME COLUMN total_quantity TO sum_quantity;
SELECT * FROM iceberg.silver.big_order_customers LIMIT 5;
DELETE FROM iceberg.silver.big_order_customers WHERE totalprice > 400000;

SELECT count(*) FROM iceberg.bronze.lineitem;  -- должно быть 6001215

SET SESSION iceberg.compression_codec = 'ZSTD';
CREATE TABLE iceberg.bronze.lineitem_zstd AS SELECT * FROM tpch.sf1.lineitem;

SET SESSION iceberg.compression_codec = 'SNAPPY';
CREATE TABLE iceberg.bronze.lineitem_snappy AS SELECT * FROM tpch.sf1.lineitem;

SET SESSION iceberg.compression_codec = 'GZIP';
CREATE TABLE iceberg.bronze.lineitem_gzip AS SELECT * FROM tpch.sf1.lineitem;

DELETE FROM iceberg.silver.big_order_customers WHERE totalprice > 400000;