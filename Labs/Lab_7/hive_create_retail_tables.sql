
-- $Id: hive_create_retail_tables.sql,v 1.1 2013/04/18 16:16:33 mgoddard Exp mgoddard $

DROP DATABASE IF EXISTS retail_demo CASCADE;
CREATE DATABASE IF NOT EXISTS retail_demo;

DROP TABLE IF EXISTS retail_demo.order_lineitems;
CREATE TABLE retail_demo.order_lineitems
(
  Order_ID                      string
, Order_Item_ID                 bigint
, Product_ID                    int
, Product_Name                  string
, Customer_ID                   int
, Store_ID                      int
, Item_Shipment_Status_Code     string
, Order_Datetime                timestamp
, Ship_Datetime                 timestamp
, Item_Return_Datetime          timestamp
, Item_Refund_Datetime          timestamp
, Product_Category_ID           int
, Product_Category_Name         string
, Payment_Method_Code           string
, Tax_Amount                    double
, Item_Quantity                 int
, Item_Price                    double
, Discount_Amount               double
, Coupon_Code                   string
, Coupon_Amount                 double
, Ship_Address_Line1            string
, Ship_Address_Line2            string
, Ship_Address_Line3            string
, Ship_Address_City             string
, Ship_Address_State            string
, Ship_Address_Postal_Code      string
, Ship_Address_Country          string
, Ship_Phone_Number             string
, Ship_Customer_Name            string
, Ship_Customer_Email_Address   string
, Ordering_Session_ID           string
, Website_URL                   string
)
  -- PARTITIONED BY (Order_Datetime timestamp)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/order_lineitems/';


DROP TABLE IF EXISTS retail_demo.orders;

CREATE TABLE retail_demo.orders
(
  Order_ID                      string
, Customer_ID                   int
, Store_ID                      int
-- , Order_Datetime_Orig           timestamp
, Order_Datetime                timestamp
, Ship_Completion_Datetime      timestamp
, Return_Datetime               timestamp
, Refund_Datetime               timestamp
, Payment_Method_Code           string
, Total_Tax_Amount              double
, Total_Paid_Amount             double
, Total_Item_Quantity           int
, Total_Discount_Amount         double
, Coupon_Code                   string
, Coupon_Amount                 double
, Order_Canceled_Flag           string
, Has_Returned_Items_Flag       string
, Has_Refunded_Items_Flag       string
, Fraud_Code                    string
, Fraud_Resolution_Code         string
, Billing_Address_Line1         string
, Billing_Address_Line2         string
, Billing_Address_Line3         string
, Billing_Address_City          string
, Billing_Address_State         string
, Billing_Address_Postal_Code   string
, Billing_Address_Country       string
, Billing_Phone_Number          string
, Customer_Name                 string
, Customer_Email_Address        string
, Ordering_Session_ID           string
, Website_URL                   string
)
  -- PARTITIONED BY (Order_Datetime timestamp)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/orders/';

DROP TABLE IF EXISTS retail_demo.products_dim;
CREATE TABLE retail_demo.products_dim
(
  Product_ID      int,
  Category_ID     smallint,
  Price           double,
  Product_Name    string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/products_dim/';

DROP TABLE IF EXISTS retail_demo.categories_dim;
CREATE TABLE retail_demo.categories_dim
(
  Category_ID    bigint,
  Category_Name  string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/categories_dim/';

DROP TABLE IF EXISTS retail_demo.email_addresses_dim;
CREATE TABLE retail_demo.email_addresses_dim
(
  Customer_ID     int,
  Email_Address   string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/email_addresses_dim/';

DROP TABLE IF EXISTS retail_demo.date_dim;
CREATE TABLE retail_demo.date_dim
(
  calendar_day      timestamp,
  reporting_year    smallint,
  reporting_quarter smallint,
  reporting_month   smallint,
  reporting_week    smallint,
  reporting_dow     smallint
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/date_dim/';

DROP TABLE IF EXISTS retail_demo.customers_dim;
CREATE TABLE retail_demo.customers_dim
(
  Customer_ID    bigint,
  First_Name     string,
  Last_Name      string,
  Gender         string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/customers_dim/';

DROP TABLE IF EXISTS retail_demo.payment_methods;
CREATE TABLE retail_demo.payment_methods
(
  Payment_Method_ID    SMALLINT,
  Payment_Method_Code  string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/payment_methods/';


DROP TABLE IF EXISTS retail_demo.customer_addresses_dim;
CREATE TABLE retail_demo.customer_addresses_dim
(
  Customer_Address_ID  bigint,
  Customer_ID          bigint,
  Valid_From_Timestamp timestamp,
  Valid_To_Timestamp   timestamp,
  House_Number         string,
  Street_Name          string,
  Appt_Suite_No        string,
  City                 string,
  State_Code           string,
  Zip_Code             string,
  Zip_Plus_Four        string,
  Country              string,
  Phone_Number         string
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  LOCATION '/retail_demo/payment_methods/';

