-- $Id: retail_demo_GPXF.sql,v 1.6 2013/04/23 02:06:07 mgoddard Exp mgoddard $

\set STOP_ON_ERROR

-- 0. Create a schema, so we have all this stuff within a single namespace and it's simpler
--    to remove it all at once.
DROP SCHEMA IF EXISTS retail_demo CASCADE;
CREATE SCHEMA retail_demo;

-- 1. HAWQ table; load via COPY in Lab #4
CREATE TABLE retail_demo.categories_dim
(
    category_id integer NOT NULL,
    category_name character varying(400) NOT NULL
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 2. GPXF/HBase table; load data into HDFS, then use importtsv to bulk load HBase
CREATE EXTERNAL TABLE retail_demo.customers_dim
(
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    gender TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/customers_dim/customers_dim.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 3. GPXF/HDFS table; load data into HDFS
CREATE EXTERNAL TABLE retail_demo.order_lineitems
(
    order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    product_name TEXT,
    customer_id TEXT,
    store_id TEXT,
    item_shipment_status_code TEXT,
    order_datetime TEXT,
    ship_datetime TEXT,
    item_return_datetime TEXT,
    item_refund_datetime TEXT,
    product_category_id TEXT,
    product_category_name TEXT,
    payment_method_code TEXT,
    tax_amount TEXT,
    item_quantity TEXT,
    item_price TEXT,
    discount_amount TEXT,
    coupon_code TEXT,
    coupon_amount TEXT,
    ship_address_line1 TEXT,
    ship_address_line2 TEXT,
    ship_address_line3 TEXT,
    ship_address_city TEXT,
    ship_address_state TEXT,
    ship_address_postal_code TEXT,
    ship_address_country TEXT,
    ship_phone_number TEXT,
    ship_customer_name TEXT,
    ship_customer_email_address TEXT,
    ordering_session_id TEXT,
    website_url TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/order_lineitems/order_lineitems.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 4. GPXF/HDFS table; load data into HDFS
CREATE EXTERNAL TABLE retail_demo.orders
(
    order_id TEXT,
    customer_id TEXT,
    store_id TEXT,
    order_datetime TEXT,
    ship_completion_datetime TEXT,
    return_datetime TEXT,
    refund_datetime TEXT,
    payment_method_code TEXT,
    total_tax_amount TEXT,
    total_paid_amount TEXT,
    total_item_quantity TEXT,
    total_discount_amount TEXT,
    coupon_code TEXT,
    coupon_amount TEXT,
    order_canceled_flag TEXT,
    has_returned_items_flag TEXT,
    has_refunded_items_flag TEXT,
    fraud_code TEXT,
    fraud_resolution_code TEXT,
    billing_address_line1 TEXT,
    billing_address_line2 TEXT,
    billing_address_line3 TEXT,
    billing_address_city TEXT,
    billing_address_state TEXT,
    billing_address_postal_code TEXT,
    billing_address_country TEXT,
    billing_phone_number TEXT,
    customer_name TEXT,
    customer_email_address TEXT,
    ordering_session_id TEXT,
    website_url TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/orders/orders.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 5. GPXF/HBase table; load data into HDFS, then use importtsv to bulk load HBase
CREATE EXTERNAL TABLE retail_demo.customer_addresses_dim
(
    customer_address_id TEXT,
    customer_id TEXT,
    valid_from_timestamp TEXT,
    valid_to_timestamp TEXT,
    house_number TEXT,
    street_name TEXT,
    appt_suite_no TEXT,
    city TEXT,
    state_code TEXT,
    zip_code TEXT,
    zip_plus_four TEXT,
    country TEXT,
    phone_number TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/customer_addresses_dim/customer_addresses_dim.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 6. HAWQ table; load via COPY
CREATE TABLE retail_demo.date_dim
(
    calendar_day date,
    reporting_year smallint,
    reporting_quarter smallint,
    reporting_month smallint,
    reporting_week smallint,
    reporting_dow smallint
)
WITH (appendonly=true) DISTRIBUTED RANDOMLY;

-- 7. GPXF/HBase table; load data into HDFS, then use importtsv to bulk load HBase
CREATE EXTERNAL TABLE retail_demo.email_addresses_dim
(
    customer_id TEXT,
    email_address TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/email_addresses_dim/email_addresses_dim.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 8. HAWQ table; load via COPY
CREATE TABLE retail_demo.payment_methods
(
    payment_method_id smallint,
    payment_method_code character varying(20)
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;
ALTER TABLE retail_demo.payment_methods OWNER TO gpadmin;

-- 9. GPXF/HBase table; load data into HDFS, then use importtsv to bulk load HBase
CREATE EXTERNAL TABLE retail_demo.products_dim
(
    product_id TEXT,
    category_id TEXT,
    price TEXT,
    product_name TEXT
)
LOCATION ('gpxf://pivhdsne:50070/retail_demo/products_dim/products_dim.tsv.gz?Fragmenter=HdfsDataFragmenter')
FORMAT 'TEXT' (DELIMITER = E'\t');

-- 10. HAWQ table, copy of order_lineitems, loaded via INSERT INTO ... SELECT ... FROM
-- but note the need to cast each of the non-TEXT columns
CREATE TABLE retail_demo.order_lineitems_hawq
(
    order_id character varying(21),
    order_item_id bigint NOT NULL,
    product_id integer,
    product_name character varying(2000),
    customer_id integer,
    store_id integer,
    item_shipment_status_code character varying(30),
    order_datetime timestamp without time zone,
    ship_datetime timestamp without time zone,
    item_return_datetime timestamp without time zone,
    item_refund_datetime timestamp without time zone,
    product_category_id integer,
    product_category_name character varying(200),
    payment_method_code character varying(20),
    tax_amount numeric(15,5),
    item_quantity integer,
    item_price numeric(10,2),
    discount_amount numeric(15,5),
    coupon_code character varying(20),
    coupon_amount numeric(15,5),
    ship_address_line1 character varying(200),
    ship_address_line2 character varying(200),
    ship_address_line3 character varying(200),
    ship_address_city character varying(200),
    ship_address_state character varying(200),
    ship_address_postal_code character varying(20),
    ship_address_country character varying(200),
    ship_phone_number character varying(20),
    ship_customer_name character varying(200),
    ship_customer_email_address character varying(200),
    ordering_session_id character varying(30),
    website_url character varying(500)
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 11. HAWQ table, copy of order_lineitems, loaded via INSERT INTO ... SELECT ... FROM
-- but note the need to cast each of the non-TEXT columns
CREATE TABLE retail_demo.orders_hawq
(
    order_id character varying(21),
    customer_id integer,
    store_id integer,
    order_datetime timestamp without time zone,
    ship_completion_datetime timestamp without time zone,
    return_datetime timestamp without time zone,
    refund_datetime timestamp without time zone,
    payment_method_code character varying(20),
    total_tax_amount numeric(15,5),
    total_paid_amount numeric(15,5),
    total_item_quantity integer,
    total_discount_amount numeric(15,5),
    coupon_code character varying(20),
    coupon_amount numeric(15,5),
    order_canceled_flag character varying(1),
    has_returned_items_flag character varying(1),
    has_refunded_items_flag character varying(1),
    fraud_code character varying(40),
    fraud_resolution_code character varying(40),
    billing_address_line1 character varying(200),
    billing_address_line2 character varying(200),
    billing_address_line3 character varying(200),
    billing_address_city character varying(200),
    billing_address_state character varying(200),
    billing_address_postal_code character varying(20),
    billing_address_country character varying(200),
    billing_phone_number character varying(20),
    customer_name character varying(200),
    customer_email_address character varying(200),
    ordering_session_id character varying(30),
    website_url character varying(500)
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

