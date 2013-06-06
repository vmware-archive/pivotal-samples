#!/bin/bash

customers_dim_gpxf=`psql -Atc "select count(*) from retail_demo.customers_dim_gpxf;"`
categories_dim_gpxf=`psql -Atc "select count(*) from retail_demo.categories_dim_gpxf;"`    
customer_addresses_dim_gpxf=`psql -Atc "select count(*) from retail_demo.customer_addresses_dim_gpxf;"`       
date_dim_gpxf=`psql -Atc "select count(*) from retail_demo.date_dim_gpxf;"`           
email_addresses_dim_gpxf=`psql -Atc "select count(*) from retail_demo.email_addresses_dim_gpxf;"`
order_lineitems_gpxf=`psql -Atc "select count(*) from retail_demo.order_lineitems_gpxf;"`   
orders_gpxf=`psql -Atc "select count(*) from retail_demo.orders_gpxf;"`         
payment_methods_gpxf=`psql -Atc "select count(*) from retail_demo.payment_methods_gpxf;"`
products_dim_gpxf=`psql -Atc "select count(*) from retail_demo.products_dim_gpxf;"`

echo "							    "
echo "        Table Name           |    Count "
echo "-----------------------------+------------------------"
echo " customers_dim_gpxf          |   $customers_dim_gpxf  "
echo " categories_dim_gpxf         |   $categories_dim_gpxf "
echo " customer_addresses_dim_gpxf |   $customer_addresses_dim_gpxf"
echo " email_addresses_dim_gpxf    |   $email_addresses_dim_gpxf"
echo " order_lineitems_gpxf        |   $order_lineitems_gpxf"
echo " orders_gpxf                 |   $orders_gpxf"
echo " payment_methods_gpxf        |   $payment_methods_gpxf"
echo " products_dim_gpxf           |   $products_dim_gpxf"
echo "-----------------------------+------------------------"
