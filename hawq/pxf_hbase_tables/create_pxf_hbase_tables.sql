CREATE EXTERNAL TABLE retail_demo.categories_dim_hbase
(
    --category_id integer,
    recordkey integer,
    "cf1:category_name" character(400)
)
LOCATION ('pxf://pivhdsne:50070/categories_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


CREATE EXTERNAL TABLE retail_demo.customers_dim_hbase
(
    -- customer_id integer,
    recordkey integer,
    "cf1:first_name" TEXT,
    "cf1:last_name" TEXT,
    "cf1:gender" character(1)
)
LOCATION ('pxf://pivhdsne:50070/customers_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


CREATE  EXTERNAL TABLE retail_demo.order_lineitems_hbase
(
    recordkey integer,
    "cf1:order_id" TEXT,
    "cf1:order_item_id" TEXT,
    "cf1:product_id" TEXT,
    "cf1:product_name" TEXT,
    "cf1:customer_id" TEXT,
    "cf1:store_id" TEXT,
    "cf1:item_shipment_status_code" TEXT,
    "cf1:order_datetime" TEXT,
    "cf1:ship_datetime" TEXT,
    "cf1:item_return_datetime" TEXT,
    "cf1:item_refund_datetime" TEXT,
    "cf1:product_category_id" TEXT,
    "cf1:product_category_name" TEXT,
    "cf1:payment_method_code" TEXT,
    "cf1:tax_amount" TEXT,
    "cf1:item_quantity" TEXT,
    "cf1:item_price" TEXT,
    "cf1:discount_amount" TEXT,
    "cf1:coupon_code" TEXT,
    "cf1:coupon_amount" TEXT,
    "cf1:ship_address_line1" TEXT,
    "cf1:ship_address_line2" TEXT,
    "cf1:ship_address_line3" TEXT,
    "cf1:ship_address_city" TEXT,
    "cf1:ship_address_state" TEXT,
    "cf1:ship_address_postal_code" TEXT,
    "cf1:ship_address_country" TEXT,
    "cf1:ship_phone_number" TEXT,
    "cf1:ship_customer_name" TEXT,
    "cf1:ship_customer_email_address" TEXT,
    "cf1:ordering_session_id" TEXT,
    "cf1:website_url" TEXT
)
LOCATION ('pxf://pivhdsne:50070/order_lineitems?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


CREATE EXTERNAL TABLE retail_demo.orders_hbase
(
    -- order_id TEXT,
    recordkey integer,
    "cf1:order_id" TEXT,
    "cf1:customer_id" TEXT,
    "cf1:store_id" TEXT,
    "cf1:order_datetime" TEXT,
    "cf1:ship_completion_datetime" TEXT,
    "cf1:return_datetime" TEXT,
    "cf1:refund_datetime" TEXT,
    "cf1:payment_method_code" TEXT,
    "cf1:total_tax_amount" TEXT,
    "cf1:total_paid_amount" TEXT,
    "cf1:total_item_quantity" TEXT,
    "cf1:total_discount_amount" TEXT,
    "cf1:coupon_code" TEXT,
    "cf1:coupon_amount" TEXT,
    "cf1:order_canceled_flag" TEXT,
    "cf1:has_returned_items_flag" TEXT,
    "cf1:has_refunded_items_flag" TEXT,
    "cf1:fraud_code" TEXT,
    "cf1:fraud_resolution_code" TEXT,
    "cf1:billing_address_line1" TEXT,
    "cf1:billing_address_line2" TEXT,
    "cf1:billing_address_line3" TEXT,
    "cf1:billing_address_city" TEXT,
    "cf1:billing_address_state" TEXT,
    "cf1:billing_address_postal_code" TEXT,
    "cf1:billing_address_country" TEXT,
    "cf1:billing_phone_number" TEXT,
    "cf1:customer_name" TEXT,
    "cf1:customer_email_address" TEXT,
    "cf1:ordering_session_id" TEXT,
    "cf1:website_url" TEXT
)
LOCATION ('pxf://pivhdsne:50070/orders?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


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
LOCATION ('pxf://pivhdsne:50070/customer_addresses_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


CREATE EXTERNAL TABLE retail_demo.date_dim_hbase
(
    recordkey integer,
    "cf1:calendar_day" TEXT,
    "cf1:reporting_year" integer,
    "cf1:reporting_quarter" integer,
    "cf1:reporting_month" integer,
    "cf1:reporting_week" integer,
    "cf1:reporting_dow" integer
)
LOCATION ('pxf://pivhdsne:50070/date_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

CREATE EXTERNAL TABLE retail_demo.email_addresses_dim_hbase
(
    -- customer_id integer,
    recordkey integer,
    "cf1:email_address" TEXT
)
LOCATION ('pxf://pivhdsne:50070/email_addresses_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


CREATE EXTERNAL TABLE retail_demo.payment_methods_hbase
(
    --payment_method_id smallint,
    recordkey integer,
    "cf1:payment_method_code" character(20)
)
LOCATION ('pxf://pivhdsne:50070/payment_methods?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');

CREATE EXTERNAL TABLE retail_demo.products_dim_hbase
(
    -- product_id integer,
    recordkey integer,
    "cf1:category_id" integer,
    -- "cf1:price" numeric(15,2),
    "cf1:price" TEXT,
    "cf1:product_name" TEXT
)
LOCATION ('pxf://pivhdsne:50070/products_dim?FRAGMENTER=HBaseDataFragmenter&Accessor=HBaseAccessor&Resolver=HBaseResolver')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');


