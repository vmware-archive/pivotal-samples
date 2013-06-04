-- $Id: retail_demo_HBase.sql,v 1.9 2013/04/23 16:14:15 mgoddard Exp mgoddard $

/*
 * NOTE: each of these tables will require the mapping table so we can avoid
 * naming our columns like "cf1:customer_id" in the queries.
 */

-- 4 HBase / GPXF tables
DROP EXTERNAL TABLE IF EXISTS retail_demo.customer_addresses_dim_hbase CASCADE;
CREATE EXTERNAL TABLE retail_demo.customer_addresses_dim_hbase
(
    recordkey integer,
    "cf1:customer_id" integer,
    -- "cf1:valid_from_timestamp" timestamp without time zone,
    "cf1:valid_from_timestamp" TEXT,
    -- "cf1:valid_to_timestamp" timestamp without time zone,
    "cf1:valid_to_timestamp" TEXT,
    "cf1:house_number" TEXT,
    "cf1:street_name" TEXT,
    "cf1:appt_suite_no" TEXT,
    "cf1:city" TEXT,
    "cf1:state_code" TEXT,
    "cf1:zip_code" TEXT,
    "cf1:zip_plus_four" TEXT,
    "cf1:country" TEXT,
    "cf1:phone_number" TEXT
)
LOCATION ('gpxf://pivhdsne:50070/customer_addresses_dim?FRAGMENTER=HBaseDataFragmenter')
FORMAT 'CUSTOM' (formatter='gpxfwritable_import');

-- Drop the ext table first
DROP EXTERNAL TABLE IF EXISTS retail_demo.customer_addresses_dim;
CREATE OR REPLACE VIEW retail_demo.customer_addresses_dim
(
    customer_address_id,
    customer_id,
    valid_from_timestamp,
    valid_to_timestamp,
    house_number,
    street_name,
    appt_suite_no,
    city,
    state_code,
    zip_code,
    zip_plus_four,
    country,
    phone_number
) AS SELECT
    recordkey,
    "cf1:customer_id",
    (CASE WHEN "cf1:valid_from_timestamp" = '' THEN NULL ELSE  "cf1:valid_from_timestamp" END)::timestamp without time zone,
    (CASE WHEN "cf1:valid_to_timestamp" = '' THEN NULL ELSE  "cf1:valid_to_timestamp" END)::timestamp without time zone,
    "cf1:house_number",
    "cf1:street_name",
    "cf1:appt_suite_no",
    "cf1:city",
    "cf1:state_code",
    "cf1:zip_code",
    "cf1:zip_plus_four",
    "cf1:country",
    "cf1:phone_number"
FROM retail_demo.customer_addresses_dim_hbase;

DROP EXTERNAL TABLE IF EXISTS retail_demo.customers_dim_hbase CASCADE;
CREATE EXTERNAL TABLE retail_demo.customers_dim_hbase
(
    -- customer_id integer,
    recordkey integer,
    "cf1:first_name" TEXT,
    "cf1:last_name" TEXT,
    "cf1:gender" character(1)
)
LOCATION ('gpxf://pivhdsne:50070/customers_dim?FRAGMENTER=HBaseDataFragmenter')
FORMAT 'CUSTOM' (formatter='gpxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS retail_demo.customers_dim;
CREATE OR REPLACE VIEW retail_demo.customers_dim
(
    customer_id,
    first_name,
    last_name,
    gender
) AS SELECT * FROM retail_demo.customers_dim_hbase;

DROP EXTERNAL TABLE IF EXISTS retail_demo.email_addresses_dim_hbase CASCADE;
CREATE EXTERNAL TABLE retail_demo.email_addresses_dim_hbase
(
    -- customer_id integer,
    recordkey integer,
    "cf1:email_address" TEXT
)
LOCATION ('gpxf://pivhdsne:50070/email_addresses_dim?FRAGMENTER=HBaseDataFragmenter')
FORMAT 'CUSTOM' (formatter='gpxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS retail_demo.email_addresses_dim;
CREATE OR REPLACE VIEW retail_demo.email_addresses_dim
(
    customer_id,
    email_address
) AS SELECT * FROM retail_demo.email_addresses_dim_hbase;

DROP EXTERNAL TABLE IF EXISTS retail_demo.products_dim_hbase CASCADE;
CREATE EXTERNAL TABLE retail_demo.products_dim_hbase
(
    -- product_id integer,
    recordkey integer,
    "cf1:category_id" integer,
    -- "cf1:price" numeric(15,2),
    "cf1:price" TEXT,
    "cf1:product_name" TEXT
)
LOCATION ('gpxf://pivhdsne:50070/products_dim?FRAGMENTER=HBaseDataFragmenter')
FORMAT 'CUSTOM' (formatter='gpxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS retail_demo.products_dim;
CREATE OR REPLACE VIEW retail_demo.products_dim
(
    product_id,
    category_id,
    price,
    product_name
) AS SELECT
  recordkey,
  "cf1:category_id",
  (CASE WHEN "cf1:price" = '' THEN NULL ELSE "cf1:price" END)::numeric(15,2),
  "cf1:product_name"
FROM retail_demo.products_dim_hbase;

