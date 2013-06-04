/*
 * Here is a query which can be run in Hive or in HAWQ/GPXF, or HAWQ/AO
 * We'll try each of these, in order, and compare the run times.  Keep in
 * mind that the Hive and HAWQ/GPXF queries run directly against the same
 * .gz files which we placed into HDFS.  The HAWQ/AO query runs against the
 * data in HAWQ's native format.  Note the speedup we get there.
 */

-- Hive
select billing_address_postal_code, sum(total_paid_amount) as total, sum(total_tax_amount) as tax
from orders
group by billing_address_postal_code
order by total desc limit 10;

/*
48001	111868.31999999998	6712.099199999999
15329	107958.24	6477.4944
42714	103244.58	6194.674800000001
41030	101365.50000000003	6081.93
50223	100511.64	6030.6984
03106	83566.41	0.0
57104	77383.62999999999	3095.3452
23002	73673.66	3683.6829999999995
25703	68282.12	4096.9272
26178	66836.4	4010.1839999999997

Time taken: 98.089 seconds
*/

-- HAWQ: GPXF
select billing_address_postal_code, sum(total_paid_amount::float8) as total, sum(total_tax_amount::float8) as tax
from retail_demo.orders
group by billing_address_postal_code
order by total desc limit 10;

/*
 billing_address_postal_code |   total   |    tax    
-----------------------------+-----------+-----------
 48001                       | 111868.32 | 6712.0992
 15329                       | 107958.24 | 6477.4944
 42714                       | 103244.58 | 6194.6748
 41030                       |  101365.5 |   6081.93
 50223                       | 100511.64 | 6030.6984
 03106                       |  83566.41 |         0
 57104                       |  77383.63 | 3095.3452
 23002                       |  73673.66 |  3683.683
 25703                       |  68282.12 | 4096.9272
 26178                       |   66836.4 |  4010.184
(10 rows)

Time: 16302.047 ms (compare with Hive's 98 seconds)
*/

-- HAWQ: HAWQ AO table (first, create the AO table)
create table retail_demo.orders_ao as select * from retail_demo.orders;
/*
SELECT 512071
Time: 16615.257 ms
*/

select billing_address_postal_code, sum(total_paid_amount::float8) as total, sum(total_tax_amount::float8) as tax from retail_demo.orders_ao group by billing_address_postal_code order by total desc limit 10;

/*
 billing_address_postal_code |   total   |    tax    
-----------------------------+-----------+-----------
 48001                       | 111868.32 | 6712.0992
 15329                       | 107958.24 | 6477.4944
 42714                       | 103244.58 | 6194.6748
 41030                       |  101365.5 |   6081.93
 50223                       | 100511.64 | 6030.6984
 03106                       |  83566.41 |         0
 57104                       |  77383.63 | 3095.3452
 23002                       |  73673.66 |  3683.683
 25703                       |  68282.12 | 4096.9272
 26178                       |   66836.4 |  4010.184
(10 rows)

Time: 2023.295 ms (About 49 times faster than the Hive query)
*/

/*
 * The queries below will only run in HAWQ, not Hive, due to their use of window
 * functions (see the "OVER" part of the query).  These are shown just to illustrate
 * that the differentiation here isn't so much about performance as about full
 * SQL support.
 */

-- Runs in HAWQ, but not in Hive
-- Every customer (actually, I limited it to 10) with their first order ID/date and last order ID/date
select customer_id
,      first_order_date
,      first_order_id
,      last_order_date
,      last_order_id
from  (select customer_id
       ,      first_value(order_datetime) over (partition by customer_id order by order_datetime asc) as first_order_date
       ,      first_value(order_id) over (partition by customer_id order by order_datetime asc) as first_order_id
       ,      last_value(order_datetime) over (partition by customer_id order by order_datetime asc) as last_order_date
       ,      last_value(order_id) over (partition by customer_id order by order_datetime asc) as last_order_id
       from   retail_demo.orders
      ) base
limit 10
;

-- Top 10 items in terms of quantity sold by month
-- Runs in HAWQ
SELECT order_month, total_sold, product_name, item_rank
FROM  (SELECT TO_CHAR(order_datetime::timestamp, 'YYYY-MM') AS order_month
       ,      product_id
       ,      SUM(item_quantity::int) AS total_sold
       ,      row_number() OVER (
                PARTITION BY TO_CHAR(order_datetime::timestamp, 'YYYY-MM')
                ORDER BY SUM(item_quantity::int)
              ) AS item_rank
       -- FROM   retail_demo.order_lineitems -- Loaded in Lab 3 (just by putting file into HDFS)
       FROM   retail_demo.order_lineitems_ao -- Loaded in Lab 4
       WHERE (order_datetime::timestamp BETWEEN timestamp '2010-10-01' AND timestamp '2010-10-14 23:59:59')
       -- OR     order_datetime::timestamp BETWEEN timestamp '2010-10-06' AND timestamp '2010-10-07 23:59:59')
       GROUP BY TO_CHAR(order_datetime::timestamp, 'YYYY-MM'), product_id
      ) AS lineitems 
,      retail_demo.products_dim p
WHERE  p.product_id = lineitems.product_id
AND    lineitems.item_rank <= 10
ORDER BY item_rank, order_month, product_name
;

-- Top 10 categories in terms of items sold for all time
-- Runs in HAWQ
SELECT product_id
,      product_category_id
,      product_count
,      category_rank
FROM (SELECT product_id, product_category_id
      ,      SUM(item_quantity::int) AS product_count
      ,      row_number() OVER (PARTITION BY product_category_id ORDER BY SUM(item_quantity::int) DESC) AS category_rank
      FROM   retail_demo.order_lineitems
      GROUP BY product_id, product_category_id
     ) AS lineitems
WHERE category_rank <= 10
ORDER BY product_category_id, category_rank
;

-- Week over week sales numbers
-- Runs in HAWQ
SELECT CASE WHEN order_datetime::timestamp < timestamp '2010-10-08' THEN date_trunc('day', order_datetime::timestamp + interval ' 1 week')
            ELSE date_trunc('day', order_datetime::timestamp)
       END::date AS order_day
,      SUM(CASE WHEN order_datetime >= timestamp '2010-01-08' THEN 1 ELSE 0 END) AS num__orders_current
,      SUM(CASE WHEN order_datetime <  timestamp '2010-01-08' THEN 1 ELSE 0 END) AS num__orders_last_week
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-10-01' AND timestamp '2010-10-15 23:59:59'
GROUP BY 1
ORDER BY 1
;

