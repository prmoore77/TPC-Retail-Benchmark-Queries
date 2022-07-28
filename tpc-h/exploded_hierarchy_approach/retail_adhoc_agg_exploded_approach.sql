/* -------------------------------------------------------------------------
File:     retail_adhoc_aggregation_exploded_approach.sql
Author:   Philip Moore (philip@voltrondata.com)
Date:     28-JUL-2022
Purpose:  This query against TPC-H is intended to perform an adhoc aggregation
          with a high-cardinality GROUP BY - with large aggregation ROLLUPs along
          multiple dimensions.  It performs intermediate aggregation step - which
          groups by the customer identifier in order to determine repeat customers.

          Note: Instead of ANSI GROUP BY ROLLUP - this query uses the exploded hierarchy
                approach - detailed here: https://medium.com/learning-sql/olap-hierarchical-aggregation-with-sql-6c45ebc206d7

Pre-requisites: You MUST run dimension_table_setup.sql first to create the required
                aggregation dimension tables.

------------------------------------------------------------------------- */

CREATE OR REPLACE TEMPORARY TABLE aggregation_results_temp
AS
WITH starting_dataset AS (
    SELECT orders.o_custkey AS customer_key
         --
         , lineitem.l_partkey AS product_key
         --
         , strftime(orders.o_orderdate, '%Y%m%d') AS date_key
         --
         , orders.o_orderkey AS order_key
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
/* Change filter conditions here */
    WHERE orders.o_custkey IN (SELECT subq.descendant_node_natural_key
                               FROM customer_aggregation_dim AS subq
                               WHERE subq.ancestor_level_name = 'REGION'
                                 AND subq.ancestor_node_name = 'AMERICA'
                                 AND subq.descendant_is_leaf = TRUE)
       AND lineitem.l_partkey IN (SELECT subq.descendant_node_natural_key
                                   FROM product_aggregation_dim AS subq
                                   WHERE subq.ancestor_level_name = 'MANUFACTURER'
                                     AND subq.ancestor_node_name = 'Manufacturer#1'
                                     AND subq.descendant_is_leaf = TRUE)
)
, rollup_aggregations AS (
    SELECT customer_aggregation_dim.ancestor_level_name AS customer_level_name
         , customer_aggregation_dim.ancestor_node_id AS customer_node_id
         , customer_aggregation_dim.ancestor_node_name AS customer_node_name
         , customer_aggregation_dim.ancestor_node_sort_order AS customer_node_sort_order
         --
         , product_aggregation_dim.ancestor_level_name AS product_level_name
         , product_aggregation_dim.ancestor_node_id AS product_node_id
         , product_aggregation_dim.ancestor_node_name AS product_node_name
         , product_aggregation_dim.ancestor_node_sort_order AS product_node_sort_order
         --
         , date_aggregation_dim.ancestor_level_name AS date_level_name
         , date_aggregation_dim.ancestor_node_id AS date_node_id
         , date_aggregation_dim.ancestor_node_name AS date_node_name
         , date_aggregation_dim.ancestor_node_sort_order AS date_node_sort_order
         --
         , facts.customer_key
         --
         , COUNT (DISTINCT facts.order_key) AS count_distinct_orders
         , SUM (facts.quantity) AS sum_quantity
         , SUM (facts.extended_price) AS sum_extended_price
         , SUM (facts.discount) AS sum_discount
         , SUM (facts.tax) AS sum_tax
         , COUNT (*) AS count_star
      FROM starting_dataset AS facts
         JOIN
           customer_aggregation_dim
         ON (    facts.customer_key = customer_aggregation_dim.descendant_node_natural_key
             AND customer_aggregation_dim.descendant_is_leaf = TRUE
             AND customer_aggregation_dim.ancestor_level_name IN ('TOTAL', 'REGION', 'NATION')
            )
         JOIN
           product_aggregation_dim
         ON (    facts.product_key = product_aggregation_dim.descendant_node_natural_key
             AND product_aggregation_dim.descendant_is_leaf = TRUE
             AND product_aggregation_dim.ancestor_level_name IN ('TOTAL', 'MANUFACTURER', 'BRAND')
            )
         JOIN
           date_aggregation_dim
         ON (    facts.date_key = date_aggregation_dim.descendant_node_natural_key
             AND date_aggregation_dim.descendant_is_leaf = TRUE
             AND date_aggregation_dim.ancestor_level_name IN ('TOTAL', 'YEAR', 'QUARTER', 'MONTH')
            )
    GROUP BY
           customer_aggregation_dim.ancestor_level_name
         , customer_aggregation_dim.ancestor_node_id
         , customer_aggregation_dim.ancestor_node_name
         , customer_aggregation_dim.ancestor_node_sort_order
         --
         , product_aggregation_dim.ancestor_level_name
         , product_aggregation_dim.ancestor_node_id
         , product_aggregation_dim.ancestor_node_name
         , product_aggregation_dim.ancestor_node_sort_order
         --
         , date_aggregation_dim.ancestor_level_name
         , date_aggregation_dim.ancestor_node_id
         , date_aggregation_dim.ancestor_node_name
         , date_aggregation_dim.ancestor_node_sort_order
         , facts.customer_key /* We group by customer here so we can determine the repeat customer count */
)
SELECT customer_level_name
     , customer_node_name
     --
     , product_level_name
     , product_node_name
     --
     , date_level_name
     , date_node_name
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
       customer_level_name
     , customer_node_id
     , customer_node_name
     , customer_node_sort_order
     --
     , product_level_name
     , product_node_id
     , product_node_name
     , product_node_sort_order
     --
     , date_level_name
     , date_node_id
     , date_node_name
     , date_node_sort_order
ORDER BY customer_node_sort_order
       , product_node_sort_order
       , date_node_sort_order
;

/*
 Statistics:
 On TPC-H Scale Factor 1 (with filters intact) - results were:
 Starting dataset row count:   239,722
 Output row count:               5,635
 Tuple Count:                8,629,992
 Explosion factor:                  36
*/

select *
 from aggregation_results_temp;
