/* -------------------------------------------------------------------------
File:     retail_adhoc_aggregation.sql
Author:   Philip Moore (philip@voltrondata.com)
Date:     28-JUL-2022
Purpose:  This query against TPC-H is intended to perform an adhoc aggregation
          with a high-cardinality GROUP BY - with large aggregation ROLLUPs along
          multiple dimensions.  It performs intermediate aggregation step - which
          groups by the customer identifier in order to determine repeat customers.

------------------------------------------------------------------------- */
WITH starting_dataset AS (
    SELECT customer_region.r_regionkey AS customer_region_key
         , customer_region.r_name AS customer_region_name
         --
         , customer_nation.n_nationkey AS customer_nation_key
         , customer_nation.n_name AS customer_nation_name
         --
         , customer.c_custkey AS customer_key
         --
         , part.p_mfgr AS part_manufacturer
         , part.p_brand AS part_brand
         , part.p_partkey AS part_key
         , part.p_name AS part_name
         --
         , orders.o_orderkey AS order_key
         , EXTRACT (YEAR FROM orders.o_orderdate) AS order_year
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-Q' || EXTRACT (QUARTER FROM orders.o_orderdate) AS order_quarter
         , EXTRACT (YEAR FROM orders.o_orderdate) || '-' || EXTRACT (MONTH FROM orders.o_orderdate) AS order_month
         , orders.o_orderdate AS order_date
         , orders.o_orderstatus AS order_status
         --
         , lineitem.l_linenumber AS line_number
         , lineitem.l_quantity AS quantity
         , lineitem.l_extendedprice AS extended_price
         , lineitem.l_discount AS discount
         , lineitem.l_tax AS tax
         , lineitem.l_linestatus AS line_status
      FROM orders
        JOIN lineitem
        ON orders.o_orderkey = lineitem.l_orderkey
        JOIN customer
        ON orders.o_custkey = customer.c_custkey
        JOIN nation AS customer_nation
        ON customer.c_nationkey = customer_nation.n_nationkey
        JOIN region AS customer_region
        ON customer_nation.n_regionkey = customer_region.r_regionkey
        JOIN part
        ON lineitem.l_partkey = part.p_partkey
/* Change filter conditions here */
    WHERE customer_region.r_name = 'AMERICA'
      AND part.p_mfgr = 'Manufacturer#1'
)
, rollup_aggregations AS (
    SELECT CASE GROUPING (customer_region_name)
              WHEN 0 THEN customer_region_name
              WHEN 1 THEN '(All)'
           END AS customer_region
         --
         , CASE GROUPING (customer_nation_name)
              WHEN 0 THEN customer_nation_name
              WHEN 1 THEN '(All)'
           END AS customer_nation
         --
         , CASE GROUPING (part_manufacturer)
              WHEN 0 THEN part_manufacturer
              WHEN 1 THEN '(All)'
           END AS part_manufacturer
         , CASE GROUPING (part_brand)
              WHEN 0 THEN part_brand
              WHEN 1 THEN '(All)'
           END AS part_brand
         --
         , CASE GROUPING (order_year)
              WHEN 0 THEN order_year
              WHEN 1 THEN '(All)'
           END AS order_year
         , CASE GROUPING (order_quarter)
              WHEN 0 THEN order_quarter
              WHEN 1 THEN '(All)'
           END AS order_quarter
         , CASE GROUPING (order_month)
              WHEN 0 THEN order_month
              WHEN 1 THEN '(All)'
           END AS order_month
         --
         , customer_key
         --
         , COUNT (DISTINCT order_key) AS count_distinct_orders
         , SUM (quantity) AS sum_quantity
         , SUM (extended_price) AS sum_extended_price
         , SUM (discount) AS sum_discount
         , SUM (tax) AS sum_tax
         , COUNT (*) AS count_star
      FROM starting_dataset
    GROUP BY
        ROLLUP (customer_region_name
              , customer_nation_name
               )
      , ROLLUP (part_manufacturer
              , part_brand
               )
      , ROLLUP (order_year
              , order_quarter
              , order_month
               )
      , customer_key /* We group by customer here so we can determine the repeat customer count */
)
SELECT customer_region
     , customer_nation
     , part_manufacturer
     , part_brand
     , order_year
     , order_quarter
     , order_month
     --
     , COUNT (/* DISTINCT */ customer_key) AS count_distinct_customers /* We do NOT need DISTINCT here b/c we grouped by all keys WITH customer_key in the previous block */
     , COUNT (/* DISTINCT */ CASE WHEN count_distinct_orders > 1 THEN 1 END) AS count_distinct_repeat_customers
     , SUM (count_distinct_orders) AS count_distinct_orders /* We can simply SUM here b/c we grouped by all keys WITH customer_key in the previous block */
     , SUM (sum_quantity) AS sum_quantity
     , SUM (sum_extended_price) AS sum_extended_price
     , SUM (sum_discount) AS sum_discount
     , SUM (sum_tax) AS sum_tax
     , SUM (count_star) AS count_star
  FROM rollup_aggregations
GROUP BY
       customer_region
     , customer_nation
     , part_manufacturer
     , part_brand
     , order_year
     , order_quarter
     , order_month
;
