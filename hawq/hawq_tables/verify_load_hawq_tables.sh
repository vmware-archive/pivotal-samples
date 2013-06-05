#!/bin/bash

customers_dim_hawq=`psql -Atc "select count(*) from retail_demo.customers_dim_hawq;"`
categories_dim_hawq=`psql -Atc "select count(*) from retail_demo.categories_dim_hawq;"`    
customer_addresses_dim_hawq=`psql -Atc "select count(*) from retail_demo.customer_addresses_dim_hawq;"`       
date_dim_hawq=`psql -Atc "select count(*) from retail_demo.date_dim_hawq;"`           
email_addresses_dim_hawq=`psql -Atc "select count(*) from retail_demo.email_addresses_dim_hawq;"`
order_lineitems_hawq=`psql -Atc "select count(*) from retail_demo.order_lineitems_hawq;"`   
orders_hawq=`psql -Atc "select count(*) from retail_demo.orders_hawq;"`         
payment_methods_hawq=`psql -Atc "select count(*) from retail_demo.payment_methods_hawq;"`
products_dim_hawq=`psql -Atc "select count(*) from retail_demo.products_dim_hawq;"`

echo "							    "
echo "        Table Name           |    Count "
echo "-----------------------------+------------------------"
echo " customers_dim_hawq          |   $customers_dim_hawq  "
echo " categories_dim_hawq         |   $categories_dim_hawq "
echo " customer_addresses_dim_hawq |   $customer_addresses_dim_hawq"
echo " email_addresses_dim_hawq    |   $email_addresses_dim_hawq"
echo " order_lineitems_hawq        |   $order_lineitems_hawq"
echo " orders_hawq                 |   $orders_hawq"
echo " payment_methods_hawq        |   $payment_methods_hawq"
echo " products_dim_hawq           |   $products_dim_hawq"
echo "-----------------------------+------------------------"
