DROP SCHEMA IF EXISTS retail_demo CASCADE;
CREATE SCHEMA retail_demo;

-- 1. HAWQ table; load via COPY
CREATE TABLE retail_demo.categories_dim_hawq
(
    category_id integer NOT NULL,
    category_name character varying(400) NOT NULL
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 2.  HAWQ table; load via COPY
CREATE TABLE retail_demo.customers_dim_hawq
(
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    gender TEXT
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 3. HAWQ table; load via COPY
CREATE  TABLE retail_demo.order_lineitems_hawq
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
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 4. HAWQ table; load via COPY
CREATE TABLE retail_demo.orders_hawq
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
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 5.  HAWQ table; load via COPY
CREATE TABLE retail_demo.customer_addresses_dim_hawq
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
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

-- 6. HAWQ table; load via COPY
CREATE TABLE retail_demo.date_dim_hawq
(
    calendar_day date,
    reporting_year smallint,
    reporting_quarter smallint,
    reporting_month smallint,
    reporting_week smallint,
    reporting_dow smallint
)
WITH (appendonly=true) DISTRIBUTED RANDOMLY;

-- 7. HAWQ table
CREATE TABLE retail_demo.email_addresses_dim_hawq
(
    customer_id TEXT,
    email_address TEXT
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;


-- 8. HAWQ table; load via COPY
CREATE TABLE retail_demo.payment_methods_hawq
(
    payment_method_id smallint,
    payment_method_code character varying(20)
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;
ALTER TABLE retail_demo.payment_methods_hawq OWNER TO gpadmin;

-- 9.  HAWQ table; load via COPY
CREATE TABLE retail_demo.products_dim_hawq
(
    product_id TEXT,
    category_id TEXT,
    price TEXT,
    product_name TEXT
)
WITH (appendonly=true, compresstype=quicklz) DISTRIBUTED RANDOMLY;

