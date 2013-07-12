#!/bin/bash
customers_dim_pxf=`psql -Atc "select count(*) from retail_demo.customers_dim_pxf;"`
categories_dim_pxf=`psql -Atc "select count(*) from retail_demo.categories_dim_pxf;"`    
customer_addresses_dim_pxf=`psql -Atc "select count(*) from retail_demo.customer_addresses_dim_pxf;"`       
date_dim_pxf=`psql -Atc "select count(*) from retail_demo.date_dim_pxf;"`           
email_addresses_dim_pxf=`psql -Atc "select count(*) from retail_demo.email_addresses_dim_pxf;"`
order_lineitems_pxf=`psql -Atc "select count(*) from retail_demo.order_lineitems_pxf;"`   
orders_pxf=`psql -Atc "select count(*) from retail_demo.orders_pxf;"`         
payment_methods_pxf=`psql -Atc "select count(*) from retail_demo.payment_methods_pxf;"`
products_dim_pxf=`psql -Atc "select count(*) from retail_demo.products_dim_pxf;"`

echo "							    "
echo "        Table Name           |    Count "
echo "-----------------------------+------------------------"
echo " customers_dim_pxf          |   $customers_dim_pxf  "
echo " categories_dim_pxf         |   $categories_dim_pxf "
echo " customer_addresses_dim_pxf |   $customer_addresses_dim_pxf"
echo " email_addresses_dim_pxf    |   $email_addresses_dim_pxf"
echo " order_lineitems_pxf        |   $order_lineitems_pxf"
echo " orders_pxf                 |   $orders_pxf"
echo " payment_methods_pxf        |   $payment_methods_pxf"
echo " products_dim_pxf           |   $products_dim_pxf"
echo "-----------------------------+------------------------"
