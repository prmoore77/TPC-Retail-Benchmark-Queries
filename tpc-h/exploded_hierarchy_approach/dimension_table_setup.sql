/* ---------------------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE date_nodes (
  node_id           VARCHAR (36)
, node_natural_key  INTEGER NOT NULL
, node_name         VARCHAR (100) NOT NULL
, level_name        VARCHAR (100) NOT NULL
, parent_node_id    VARCHAR (36)
--
, CONSTRAINT date_nodes_pk PRIMARY KEY (node_id)
, CONSTRAINT date_nodes_uk_1 UNIQUE (level_name, node_natural_key)
, CONSTRAINT date_nodes_uk_2 UNIQUE (level_name, node_name)
, CONSTRAINT date_nodes_self_fk FOREIGN KEY (parent_node_id)
    REFERENCES date_nodes (node_id)
)
;

-- Top Node
INSERT INTO date_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
SELECT uuid() AS node_id
     , 0 AS node_natural_key
     , 'ALL DATES' AS node_name
     , 'TOTAL' AS level_name
     , NULL AS parent_node_id
;

CREATE OR REPLACE TEMPORARY TABLE source_data_temp
AS
WITH step1 AS (
SELECT DISTINCT
       strftime(o_orderdate, '%Y%m%d') AS day_key
     , o_orderdate::VARCHAR AS day_name
     , EXTRACT (YEAR FROM o_orderdate) AS year_key
     , EXTRACT (YEAR FROM o_orderdate) || 'Q' || EXTRACT (QUARTER FROM o_orderdate) AS quarter_name
     , EXTRACT (YEAR FROM o_orderdate) || 'M' || EXTRACT (MONTH FROM o_orderdate) AS month_name
  FROM orders
)
SELECT day_key
     , day_name
     , year_key
     , year_key::VARCHAR AS year_name
     , quarter_name
     , DENSE_RANK () OVER (ORDER BY quarter_name ASC) AS quarter_key
     , month_name
     , DENSE_RANK () OVER (ORDER BY month_name ASC) AS month_key
  FROM step1
;

INSERT INTO date_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       year_key AS node_natural_key
     , year_name AS node_name
     , 'YEAR' AS level_name
     , (SELECT date_nodes.node_id
        FROM date_nodes
        WHERE date_nodes.level_name = 'TOTAL'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO date_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       quarter_key AS node_natural_key
     , quarter_name AS node_name
     , 'QUARTER' AS level_name
     , (SELECT date_nodes.node_id
        FROM date_nodes
        WHERE date_nodes.node_natural_key = source_data_temp.year_key
          AND date_nodes.level_name = 'YEAR'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO date_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       month_key AS node_natural_key
     , month_name AS node_name
     , 'MONTH' AS level_name
     , (SELECT date_nodes.node_id
        FROM date_nodes
        WHERE date_nodes.node_natural_key = source_data_temp.quarter_key
          AND date_nodes.level_name = 'QUARTER'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO date_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       day_key AS node_natural_key
     , day_name AS node_name
     , 'DAY' AS level_name
     , (SELECT date_nodes.node_id
        FROM date_nodes
        WHERE date_nodes.node_natural_key = source_data_temp.month_key
          AND date_nodes.level_name = 'MONTH'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

CREATE OR REPLACE TABLE date_reporting_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1} AS node_json, [{node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1}] AS node_json_path
FROM (SELECT date_nodes.node_id AS node_id, date_nodes.node_natural_key AS node_natural_key, date_nodes.node_name AS node_name, date_nodes.level_name AS level_name, date_nodes.parent_node_id AS parent_node_id, CASE WHEN (date_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (date_nodes.node_id IN (SELECT date_nodes.parent_node_id
FROM date_nodes)) THEN false ELSE true END AS is_leaf
FROM date_nodes) AS nodes
WHERE nodes.is_root = true UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, parent_nodes.level_number + 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)} AS node_json, array_append(parent_nodes.node_json_path, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)}) AS node_json_path
FROM (SELECT date_nodes.node_id AS node_id, date_nodes.node_natural_key AS node_natural_key, date_nodes.node_name AS node_name, date_nodes.level_name AS level_name, date_nodes.parent_node_id AS parent_node_id, CASE WHEN (date_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (date_nodes.node_id IN (SELECT date_nodes.parent_node_id
FROM date_nodes)) THEN false ELSE true END AS is_leaf
FROM date_nodes) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
node_sort_order_query AS
(SELECT parent_nodes.node_id AS node_id, parent_nodes.node_natural_key AS node_natural_key, parent_nodes.node_name AS node_name, parent_nodes.level_name AS level_name, parent_nodes.parent_node_id AS parent_node_id, parent_nodes.is_root AS is_root, parent_nodes.is_leaf AS is_leaf, parent_nodes.level_number AS level_number, parent_nodes.node_json AS node_json, parent_nodes.node_json_path AS node_json_path, row_number() OVER (ORDER BY replace(CAST(parent_nodes.node_json_path AS VARCHAR), ']', '') ASC) AS node_sort_order
FROM parent_nodes)
 SELECT node_sort_order_query.node_id, node_sort_order_query.node_natural_key, node_sort_order_query.node_name, node_sort_order_query.level_name, node_sort_order_query.parent_node_id, node_sort_order_query.is_root, node_sort_order_query.is_leaf, node_sort_order_query.level_number, node_sort_order_query.node_sort_order, {node_id: node_id, node_natural_key: node_natural_key, node_name: node_name, level_name: level_name, parent_node_id: parent_node_id, is_root: is_root, is_leaf: is_leaf, level_number: level_number, node_sort_order: node_sort_order} AS node_json, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_id') AS level_1_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_natural_key') AS level_1_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_name') AS level_1_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_name') AS level_1_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'parent_node_id') AS level_1_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_number') AS level_1_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_id') AS level_2_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_natural_key') AS level_2_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_name') AS level_2_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_name') AS level_2_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'parent_node_id') AS level_2_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_number') AS level_2_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_id') AS level_3_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_natural_key') AS level_3_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_name') AS level_3_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_name') AS level_3_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'parent_node_id') AS level_3_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_number') AS level_3_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_id') AS level_4_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_natural_key') AS level_4_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_name') AS level_4_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_name') AS level_4_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'parent_node_id') AS level_4_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_number') AS level_4_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_id') AS level_5_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_natural_key') AS level_5_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_name') AS level_5_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_name') AS level_5_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'parent_node_id') AS level_5_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_number') AS level_5_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_id') AS level_6_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_natural_key') AS level_6_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_name') AS level_6_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_name') AS level_6_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'parent_node_id') AS level_6_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_number') AS level_6_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_id') AS level_7_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_natural_key') AS level_7_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_name') AS level_7_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_name') AS level_7_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'parent_node_id') AS level_7_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_number') AS level_7_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_id') AS level_8_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_natural_key') AS level_8_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_name') AS level_8_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_name') AS level_8_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'parent_node_id') AS level_8_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_number') AS level_8_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_id') AS level_9_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_natural_key') AS level_9_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_name') AS level_9_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_name') AS level_9_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'parent_node_id') AS level_9_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_number') AS level_9_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_id') AS level_10_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_natural_key') AS level_10_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_name') AS level_10_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_name') AS level_10_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'parent_node_id') AS level_10_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_number') AS level_10_level_number
FROM node_sort_order_query
;

CREATE OR REPLACE TABLE date_aggregation_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_sort_order, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, [nodes.node_json] AS node_json_path
FROM (SELECT date_reporting_dim.node_id AS node_id, date_reporting_dim.node_natural_key AS node_natural_key, date_reporting_dim.node_name AS node_name, date_reporting_dim.level_name AS level_name, date_reporting_dim.parent_node_id AS parent_node_id, date_reporting_dim.is_root AS is_root, date_reporting_dim.is_leaf AS is_leaf, date_reporting_dim.level_number AS level_number, date_reporting_dim.node_sort_order AS node_sort_order, date_reporting_dim.node_json AS node_json, date_reporting_dim.level_1_node_id AS level_1_node_id, date_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, date_reporting_dim.level_1_node_name AS level_1_node_name, date_reporting_dim.level_1_level_name AS level_1_level_name, date_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, date_reporting_dim.level_1_level_number AS level_1_level_number, date_reporting_dim.level_2_node_id AS level_2_node_id, date_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, date_reporting_dim.level_2_node_name AS level_2_node_name, date_reporting_dim.level_2_level_name AS level_2_level_name, date_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, date_reporting_dim.level_2_level_number AS level_2_level_number, date_reporting_dim.level_3_node_id AS level_3_node_id, date_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, date_reporting_dim.level_3_node_name AS level_3_node_name, date_reporting_dim.level_3_level_name AS level_3_level_name, date_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, date_reporting_dim.level_3_level_number AS level_3_level_number, date_reporting_dim.level_4_node_id AS level_4_node_id, date_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, date_reporting_dim.level_4_node_name AS level_4_node_name, date_reporting_dim.level_4_level_name AS level_4_level_name, date_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, date_reporting_dim.level_4_level_number AS level_4_level_number, date_reporting_dim.level_5_node_id AS level_5_node_id, date_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, date_reporting_dim.level_5_node_name AS level_5_node_name, date_reporting_dim.level_5_level_name AS level_5_level_name, date_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, date_reporting_dim.level_5_level_number AS level_5_level_number, date_reporting_dim.level_6_node_id AS level_6_node_id, date_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, date_reporting_dim.level_6_node_name AS level_6_node_name, date_reporting_dim.level_6_level_name AS level_6_level_name, date_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, date_reporting_dim.level_6_level_number AS level_6_level_number, date_reporting_dim.level_7_node_id AS level_7_node_id, date_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, date_reporting_dim.level_7_node_name AS level_7_node_name, date_reporting_dim.level_7_level_name AS level_7_level_name, date_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, date_reporting_dim.level_7_level_number AS level_7_level_number, date_reporting_dim.level_8_node_id AS level_8_node_id, date_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, date_reporting_dim.level_8_node_name AS level_8_node_name, date_reporting_dim.level_8_level_name AS level_8_level_name, date_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, date_reporting_dim.level_8_level_number AS level_8_level_number, date_reporting_dim.level_9_node_id AS level_9_node_id, date_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, date_reporting_dim.level_9_node_name AS level_9_node_name, date_reporting_dim.level_9_level_name AS level_9_level_name, date_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, date_reporting_dim.level_9_level_number AS level_9_level_number, date_reporting_dim.level_10_node_id AS level_10_node_id, date_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, date_reporting_dim.level_10_node_name AS level_10_node_name, date_reporting_dim.level_10_level_name AS level_10_level_name, date_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, date_reporting_dim.level_10_level_number AS level_10_level_number
FROM date_reporting_dim) AS nodes UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, array_append(parent_nodes.node_json_path, nodes.node_json) AS node_json_path
FROM (SELECT date_reporting_dim.node_id AS node_id, date_reporting_dim.node_natural_key AS node_natural_key, date_reporting_dim.node_name AS node_name, date_reporting_dim.level_name AS level_name, date_reporting_dim.parent_node_id AS parent_node_id, date_reporting_dim.is_root AS is_root, date_reporting_dim.is_leaf AS is_leaf, date_reporting_dim.level_number AS level_number, date_reporting_dim.node_sort_order AS node_sort_order, date_reporting_dim.node_json AS node_json, date_reporting_dim.level_1_node_id AS level_1_node_id, date_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, date_reporting_dim.level_1_node_name AS level_1_node_name, date_reporting_dim.level_1_level_name AS level_1_level_name, date_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, date_reporting_dim.level_1_level_number AS level_1_level_number, date_reporting_dim.level_2_node_id AS level_2_node_id, date_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, date_reporting_dim.level_2_node_name AS level_2_node_name, date_reporting_dim.level_2_level_name AS level_2_level_name, date_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, date_reporting_dim.level_2_level_number AS level_2_level_number, date_reporting_dim.level_3_node_id AS level_3_node_id, date_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, date_reporting_dim.level_3_node_name AS level_3_node_name, date_reporting_dim.level_3_level_name AS level_3_level_name, date_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, date_reporting_dim.level_3_level_number AS level_3_level_number, date_reporting_dim.level_4_node_id AS level_4_node_id, date_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, date_reporting_dim.level_4_node_name AS level_4_node_name, date_reporting_dim.level_4_level_name AS level_4_level_name, date_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, date_reporting_dim.level_4_level_number AS level_4_level_number, date_reporting_dim.level_5_node_id AS level_5_node_id, date_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, date_reporting_dim.level_5_node_name AS level_5_node_name, date_reporting_dim.level_5_level_name AS level_5_level_name, date_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, date_reporting_dim.level_5_level_number AS level_5_level_number, date_reporting_dim.level_6_node_id AS level_6_node_id, date_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, date_reporting_dim.level_6_node_name AS level_6_node_name, date_reporting_dim.level_6_level_name AS level_6_level_name, date_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, date_reporting_dim.level_6_level_number AS level_6_level_number, date_reporting_dim.level_7_node_id AS level_7_node_id, date_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, date_reporting_dim.level_7_node_name AS level_7_node_name, date_reporting_dim.level_7_level_name AS level_7_level_name, date_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, date_reporting_dim.level_7_level_number AS level_7_level_number, date_reporting_dim.level_8_node_id AS level_8_node_id, date_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, date_reporting_dim.level_8_node_name AS level_8_node_name, date_reporting_dim.level_8_level_name AS level_8_level_name, date_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, date_reporting_dim.level_8_level_number AS level_8_level_number, date_reporting_dim.level_9_node_id AS level_9_node_id, date_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, date_reporting_dim.level_9_node_name AS level_9_node_name, date_reporting_dim.level_9_level_name AS level_9_level_name, date_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, date_reporting_dim.level_9_level_number AS level_9_level_number, date_reporting_dim.level_10_node_id AS level_10_node_id, date_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, date_reporting_dim.level_10_node_name AS level_10_node_name, date_reporting_dim.level_10_level_name AS level_10_level_name, date_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, date_reporting_dim.level_10_level_number AS level_10_level_number
FROM date_reporting_dim) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
anon_1 AS
(SELECT struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_id') AS ancestor_node_id, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_natural_key') AS ancestor_node_natural_key, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_name') AS ancestor_node_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_name') AS ancestor_level_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_root') AS ancestor_is_root, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_leaf') AS ancestor_is_leaf, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_number') AS ancestor_level_number, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_sort_order') AS ancestor_node_sort_order, parent_nodes.node_id AS descendant_node_id, parent_nodes.node_natural_key AS descendant_node_natural_key, parent_nodes.node_name AS descendant_node_name, parent_nodes.level_name AS descendant_level_name, parent_nodes.is_root AS descendant_is_root, parent_nodes.is_leaf AS descendant_is_leaf, parent_nodes.level_number AS descendant_level_number, parent_nodes.node_sort_order AS descendant_node_sort_order
FROM parent_nodes)
 SELECT anon_1.ancestor_node_id, anon_1.ancestor_node_natural_key, anon_1.ancestor_node_name, anon_1.ancestor_level_name, anon_1.ancestor_is_root, anon_1.ancestor_is_leaf, anon_1.ancestor_level_number, anon_1.ancestor_node_sort_order, anon_1.descendant_node_id, anon_1.descendant_node_natural_key, anon_1.descendant_node_name, anon_1.descendant_level_name, anon_1.descendant_is_root, anon_1.descendant_is_leaf, anon_1.descendant_level_number, anon_1.descendant_node_sort_order, anon_1.descendant_level_number - anon_1.ancestor_level_number AS net_level
FROM anon_1
;

/* ---------------------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE customer_nodes (
  node_id           VARCHAR (36)
, node_natural_key  INTEGER NOT NULL
, node_name         VARCHAR (100) NOT NULL
, level_name        VARCHAR (100) NOT NULL
, parent_node_id    VARCHAR (36)
--
, CONSTRAINT customer_nodes_pk PRIMARY KEY (node_id)
, CONSTRAINT customer_nodes_uk_1 UNIQUE (level_name, node_natural_key)
, CONSTRAINT customer_nodes_uk_2 UNIQUE (level_name, node_name)
, CONSTRAINT customer_nodes_self_fk FOREIGN KEY (parent_node_id)
    REFERENCES customer_nodes (node_id)
)
;

-- Top Node
INSERT INTO customer_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
SELECT uuid() AS node_id
     , 0 AS node_natural_key
     , 'ALL CUSTOMERS' AS node_name
     , 'TOTAL' AS level_name
     , NULL AS parent_node_id
;


CREATE OR REPLACE TEMPORARY TABLE source_data_temp
AS
SELECT customer_region.r_regionkey AS customer_region_key
     , customer_region.r_name AS customer_region_name
     --
     , customer_nation.n_nationkey AS customer_nation_key
     , customer_nation.n_name AS customer_nation_name
     --
     , customer.c_custkey AS customer_key
     , customer.c_name AS customer_name
  FROM customer
    JOIN nation AS customer_nation
    ON customer.c_nationkey = customer_nation.n_nationkey
    JOIN region AS customer_region
    ON customer_nation.n_regionkey = customer_region.r_regionkey
;

INSERT INTO customer_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       customer_region_key AS node_natural_key
     , customer_region_name AS node_name
     , 'REGION' AS level_name
     , (SELECT customer_nodes.node_id
        FROM customer_nodes
        WHERE customer_nodes.level_name = 'TOTAL'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO customer_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       customer_nation_key AS node_natural_key
     , customer_nation_name AS node_name
     , 'NATION' AS level_name
     , (SELECT customer_nodes.node_id
        FROM customer_nodes
        WHERE customer_nodes.level_name = 'REGION'
          AND customer_nodes.node_natural_key = source_data_temp.customer_region_key
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO customer_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       customer_key AS node_natural_key
     , customer_name AS node_name
     , 'CUSTOMER' AS level_name
     , (SELECT customer_nodes.node_id
        FROM customer_nodes
        WHERE customer_nodes.level_name = 'NATION'
          AND customer_nodes.node_natural_key = source_data_temp.customer_nation_key
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

CREATE OR REPLACE TABLE customer_reporting_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1} AS node_json, [{node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1}] AS node_json_path
FROM (SELECT customer_nodes.node_id AS node_id, customer_nodes.node_natural_key AS node_natural_key, customer_nodes.node_name AS node_name, customer_nodes.level_name AS level_name, customer_nodes.parent_node_id AS parent_node_id, CASE WHEN (customer_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (customer_nodes.node_id IN (SELECT customer_nodes.parent_node_id
FROM customer_nodes)) THEN false ELSE true END AS is_leaf
FROM customer_nodes) AS nodes
WHERE nodes.is_root = true UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, parent_nodes.level_number + 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)} AS node_json, array_append(parent_nodes.node_json_path, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)}) AS node_json_path
FROM (SELECT customer_nodes.node_id AS node_id, customer_nodes.node_natural_key AS node_natural_key, customer_nodes.node_name AS node_name, customer_nodes.level_name AS level_name, customer_nodes.parent_node_id AS parent_node_id, CASE WHEN (customer_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (customer_nodes.node_id IN (SELECT customer_nodes.parent_node_id
FROM customer_nodes)) THEN false ELSE true END AS is_leaf
FROM customer_nodes) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
node_sort_order_query AS
(SELECT parent_nodes.node_id AS node_id, parent_nodes.node_natural_key AS node_natural_key, parent_nodes.node_name AS node_name, parent_nodes.level_name AS level_name, parent_nodes.parent_node_id AS parent_node_id, parent_nodes.is_root AS is_root, parent_nodes.is_leaf AS is_leaf, parent_nodes.level_number AS level_number, parent_nodes.node_json AS node_json, parent_nodes.node_json_path AS node_json_path, row_number() OVER (ORDER BY replace(CAST(parent_nodes.node_json_path AS VARCHAR), ']', '') ASC) AS node_sort_order
FROM parent_nodes)
 SELECT node_sort_order_query.node_id, node_sort_order_query.node_natural_key, node_sort_order_query.node_name, node_sort_order_query.level_name, node_sort_order_query.parent_node_id, node_sort_order_query.is_root, node_sort_order_query.is_leaf, node_sort_order_query.level_number, node_sort_order_query.node_sort_order, {node_id: node_id, node_natural_key: node_natural_key, node_name: node_name, level_name: level_name, parent_node_id: parent_node_id, is_root: is_root, is_leaf: is_leaf, level_number: level_number, node_sort_order: node_sort_order} AS node_json, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_id') AS level_1_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_natural_key') AS level_1_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_name') AS level_1_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_name') AS level_1_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'parent_node_id') AS level_1_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_number') AS level_1_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_id') AS level_2_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_natural_key') AS level_2_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_name') AS level_2_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_name') AS level_2_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'parent_node_id') AS level_2_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_number') AS level_2_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_id') AS level_3_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_natural_key') AS level_3_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_name') AS level_3_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_name') AS level_3_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'parent_node_id') AS level_3_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_number') AS level_3_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_id') AS level_4_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_natural_key') AS level_4_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_name') AS level_4_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_name') AS level_4_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'parent_node_id') AS level_4_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_number') AS level_4_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_id') AS level_5_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_natural_key') AS level_5_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_name') AS level_5_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_name') AS level_5_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'parent_node_id') AS level_5_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_number') AS level_5_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_id') AS level_6_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_natural_key') AS level_6_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_name') AS level_6_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_name') AS level_6_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'parent_node_id') AS level_6_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_number') AS level_6_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_id') AS level_7_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_natural_key') AS level_7_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_name') AS level_7_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_name') AS level_7_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'parent_node_id') AS level_7_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_number') AS level_7_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_id') AS level_8_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_natural_key') AS level_8_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_name') AS level_8_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_name') AS level_8_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'parent_node_id') AS level_8_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_number') AS level_8_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_id') AS level_9_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_natural_key') AS level_9_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_name') AS level_9_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_name') AS level_9_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'parent_node_id') AS level_9_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_number') AS level_9_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_id') AS level_10_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_natural_key') AS level_10_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_name') AS level_10_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_name') AS level_10_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'parent_node_id') AS level_10_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_number') AS level_10_level_number
FROM node_sort_order_query
;

CREATE OR REPLACE TABLE customer_aggregation_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_sort_order, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, [nodes.node_json] AS node_json_path
FROM (SELECT customer_reporting_dim.node_id AS node_id, customer_reporting_dim.node_natural_key AS node_natural_key, customer_reporting_dim.node_name AS node_name, customer_reporting_dim.level_name AS level_name, customer_reporting_dim.parent_node_id AS parent_node_id, customer_reporting_dim.is_root AS is_root, customer_reporting_dim.is_leaf AS is_leaf, customer_reporting_dim.level_number AS level_number, customer_reporting_dim.node_sort_order AS node_sort_order, customer_reporting_dim.node_json AS node_json, customer_reporting_dim.level_1_node_id AS level_1_node_id, customer_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, customer_reporting_dim.level_1_node_name AS level_1_node_name, customer_reporting_dim.level_1_level_name AS level_1_level_name, customer_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, customer_reporting_dim.level_1_level_number AS level_1_level_number, customer_reporting_dim.level_2_node_id AS level_2_node_id, customer_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, customer_reporting_dim.level_2_node_name AS level_2_node_name, customer_reporting_dim.level_2_level_name AS level_2_level_name, customer_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, customer_reporting_dim.level_2_level_number AS level_2_level_number, customer_reporting_dim.level_3_node_id AS level_3_node_id, customer_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, customer_reporting_dim.level_3_node_name AS level_3_node_name, customer_reporting_dim.level_3_level_name AS level_3_level_name, customer_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, customer_reporting_dim.level_3_level_number AS level_3_level_number, customer_reporting_dim.level_4_node_id AS level_4_node_id, customer_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, customer_reporting_dim.level_4_node_name AS level_4_node_name, customer_reporting_dim.level_4_level_name AS level_4_level_name, customer_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, customer_reporting_dim.level_4_level_number AS level_4_level_number, customer_reporting_dim.level_5_node_id AS level_5_node_id, customer_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, customer_reporting_dim.level_5_node_name AS level_5_node_name, customer_reporting_dim.level_5_level_name AS level_5_level_name, customer_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, customer_reporting_dim.level_5_level_number AS level_5_level_number, customer_reporting_dim.level_6_node_id AS level_6_node_id, customer_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, customer_reporting_dim.level_6_node_name AS level_6_node_name, customer_reporting_dim.level_6_level_name AS level_6_level_name, customer_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, customer_reporting_dim.level_6_level_number AS level_6_level_number, customer_reporting_dim.level_7_node_id AS level_7_node_id, customer_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, customer_reporting_dim.level_7_node_name AS level_7_node_name, customer_reporting_dim.level_7_level_name AS level_7_level_name, customer_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, customer_reporting_dim.level_7_level_number AS level_7_level_number, customer_reporting_dim.level_8_node_id AS level_8_node_id, customer_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, customer_reporting_dim.level_8_node_name AS level_8_node_name, customer_reporting_dim.level_8_level_name AS level_8_level_name, customer_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, customer_reporting_dim.level_8_level_number AS level_8_level_number, customer_reporting_dim.level_9_node_id AS level_9_node_id, customer_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, customer_reporting_dim.level_9_node_name AS level_9_node_name, customer_reporting_dim.level_9_level_name AS level_9_level_name, customer_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, customer_reporting_dim.level_9_level_number AS level_9_level_number, customer_reporting_dim.level_10_node_id AS level_10_node_id, customer_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, customer_reporting_dim.level_10_node_name AS level_10_node_name, customer_reporting_dim.level_10_level_name AS level_10_level_name, customer_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, customer_reporting_dim.level_10_level_number AS level_10_level_number
FROM customer_reporting_dim) AS nodes UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, array_append(parent_nodes.node_json_path, nodes.node_json) AS node_json_path
FROM (SELECT customer_reporting_dim.node_id AS node_id, customer_reporting_dim.node_natural_key AS node_natural_key, customer_reporting_dim.node_name AS node_name, customer_reporting_dim.level_name AS level_name, customer_reporting_dim.parent_node_id AS parent_node_id, customer_reporting_dim.is_root AS is_root, customer_reporting_dim.is_leaf AS is_leaf, customer_reporting_dim.level_number AS level_number, customer_reporting_dim.node_sort_order AS node_sort_order, customer_reporting_dim.node_json AS node_json, customer_reporting_dim.level_1_node_id AS level_1_node_id, customer_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, customer_reporting_dim.level_1_node_name AS level_1_node_name, customer_reporting_dim.level_1_level_name AS level_1_level_name, customer_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, customer_reporting_dim.level_1_level_number AS level_1_level_number, customer_reporting_dim.level_2_node_id AS level_2_node_id, customer_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, customer_reporting_dim.level_2_node_name AS level_2_node_name, customer_reporting_dim.level_2_level_name AS level_2_level_name, customer_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, customer_reporting_dim.level_2_level_number AS level_2_level_number, customer_reporting_dim.level_3_node_id AS level_3_node_id, customer_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, customer_reporting_dim.level_3_node_name AS level_3_node_name, customer_reporting_dim.level_3_level_name AS level_3_level_name, customer_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, customer_reporting_dim.level_3_level_number AS level_3_level_number, customer_reporting_dim.level_4_node_id AS level_4_node_id, customer_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, customer_reporting_dim.level_4_node_name AS level_4_node_name, customer_reporting_dim.level_4_level_name AS level_4_level_name, customer_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, customer_reporting_dim.level_4_level_number AS level_4_level_number, customer_reporting_dim.level_5_node_id AS level_5_node_id, customer_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, customer_reporting_dim.level_5_node_name AS level_5_node_name, customer_reporting_dim.level_5_level_name AS level_5_level_name, customer_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, customer_reporting_dim.level_5_level_number AS level_5_level_number, customer_reporting_dim.level_6_node_id AS level_6_node_id, customer_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, customer_reporting_dim.level_6_node_name AS level_6_node_name, customer_reporting_dim.level_6_level_name AS level_6_level_name, customer_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, customer_reporting_dim.level_6_level_number AS level_6_level_number, customer_reporting_dim.level_7_node_id AS level_7_node_id, customer_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, customer_reporting_dim.level_7_node_name AS level_7_node_name, customer_reporting_dim.level_7_level_name AS level_7_level_name, customer_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, customer_reporting_dim.level_7_level_number AS level_7_level_number, customer_reporting_dim.level_8_node_id AS level_8_node_id, customer_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, customer_reporting_dim.level_8_node_name AS level_8_node_name, customer_reporting_dim.level_8_level_name AS level_8_level_name, customer_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, customer_reporting_dim.level_8_level_number AS level_8_level_number, customer_reporting_dim.level_9_node_id AS level_9_node_id, customer_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, customer_reporting_dim.level_9_node_name AS level_9_node_name, customer_reporting_dim.level_9_level_name AS level_9_level_name, customer_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, customer_reporting_dim.level_9_level_number AS level_9_level_number, customer_reporting_dim.level_10_node_id AS level_10_node_id, customer_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, customer_reporting_dim.level_10_node_name AS level_10_node_name, customer_reporting_dim.level_10_level_name AS level_10_level_name, customer_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, customer_reporting_dim.level_10_level_number AS level_10_level_number
FROM customer_reporting_dim) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
anon_1 AS
(SELECT struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_id') AS ancestor_node_id, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_natural_key') AS ancestor_node_natural_key, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_name') AS ancestor_node_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_name') AS ancestor_level_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_root') AS ancestor_is_root, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_leaf') AS ancestor_is_leaf, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_number') AS ancestor_level_number, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_sort_order') AS ancestor_node_sort_order, parent_nodes.node_id AS descendant_node_id, parent_nodes.node_natural_key AS descendant_node_natural_key, parent_nodes.node_name AS descendant_node_name, parent_nodes.level_name AS descendant_level_name, parent_nodes.is_root AS descendant_is_root, parent_nodes.is_leaf AS descendant_is_leaf, parent_nodes.level_number AS descendant_level_number, parent_nodes.node_sort_order AS descendant_node_sort_order
FROM parent_nodes)
 SELECT anon_1.ancestor_node_id, anon_1.ancestor_node_natural_key, anon_1.ancestor_node_name, anon_1.ancestor_level_name, anon_1.ancestor_is_root, anon_1.ancestor_is_leaf, anon_1.ancestor_level_number, anon_1.ancestor_node_sort_order, anon_1.descendant_node_id, anon_1.descendant_node_natural_key, anon_1.descendant_node_name, anon_1.descendant_level_name, anon_1.descendant_is_root, anon_1.descendant_is_leaf, anon_1.descendant_level_number, anon_1.descendant_node_sort_order, anon_1.descendant_level_number - anon_1.ancestor_level_number AS net_level
FROM anon_1
;

/* ---------------------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE product_nodes (
  node_id           VARCHAR (36)
, node_natural_key  INTEGER NOT NULL
, node_name         VARCHAR (100) NOT NULL
, level_name        VARCHAR (100) NOT NULL
, parent_node_id    VARCHAR (36)
--
, CONSTRAINT product_nodes_pk PRIMARY KEY (node_id)
, CONSTRAINT product_nodes_uk_1 UNIQUE (level_name, node_natural_key)
, CONSTRAINT product_nodes_uk_2 UNIQUE (level_name, node_name)
, CONSTRAINT product_nodes_self_fk FOREIGN KEY (parent_node_id)
    REFERENCES product_nodes (node_id)
)
;

-- Top Node
INSERT INTO product_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
SELECT uuid() AS node_id
     , 0 AS node_natural_key
     , 'ALL PRODUCTS' AS node_name
     , 'TOTAL' AS level_name
     , NULL AS parent_node_id
;

CREATE OR REPLACE TEMPORARY TABLE source_data_temp
AS
SELECT p_partkey AS part_key
     , CASE WHEN COUNT(*) OVER (PARTITION BY p_name) > 1
          THEN p_name || DENSE_RANK() OVER (PARTITION BY p_name ORDER BY p_partkey)
          ELSE p_name
       END AS part_name
     , p_mfgr AS mfgr_name
     , DENSE_RANK() OVER (ORDER BY p_mfgr ASC) AS mfgr_key
     , p_brand AS brand_name
     , DENSE_RANK() OVER (ORDER BY p_brand ASC) AS brand_key
  FROM part
;


INSERT INTO product_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       mfgr_key AS node_natural_key
     , mfgr_name AS node_name
     , 'MANUFACTURER' AS level_name
     , (SELECT product_nodes.node_id
        FROM product_nodes
        WHERE product_nodes.level_name = 'TOTAL'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO product_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       brand_key AS node_natural_key
     , brand_name AS node_name
     , 'BRAND' AS level_name
     , (SELECT product_nodes.node_id
        FROM product_nodes
        WHERE product_nodes.node_natural_key = source_data_temp.mfgr_key
          AND product_nodes.level_name = 'MANUFACTURER'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

INSERT INTO product_nodes (
  node_id
, node_natural_key
, node_name
, level_name
, parent_node_id
)
WITH raw_data AS (
SELECT DISTINCT
       part_key AS node_natural_key
     , part_name AS node_name
     , 'PART' AS level_name
     , (SELECT product_nodes.node_id
        FROM product_nodes
        WHERE product_nodes.node_natural_key = source_data_temp.brand_key
          AND product_nodes.level_name = 'BRAND'
       ) AS parent_node_id
 FROM source_data_temp
)
SELECT uuid() AS node_id
     , node_natural_key
     , node_name
     , level_name
     , parent_node_id
  FROM raw_data
;

CREATE OR REPLACE TABLE product_reporting_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1} AS node_json, [{node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: 1}] AS node_json_path
FROM (SELECT product_nodes.node_id AS node_id, product_nodes.node_natural_key AS node_natural_key, product_nodes.node_name AS node_name, product_nodes.level_name AS level_name, product_nodes.parent_node_id AS parent_node_id, CASE WHEN (product_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (product_nodes.node_id IN (SELECT product_nodes.parent_node_id
FROM product_nodes)) THEN false ELSE true END AS is_leaf
FROM product_nodes) AS nodes
WHERE nodes.is_root = true UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, parent_nodes.level_number + 1 AS level_number, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)} AS node_json, array_append(parent_nodes.node_json_path, {node_id: nodes.node_id, node_natural_key: nodes.node_natural_key, node_name: nodes.node_name, level_name: nodes.level_name, parent_node_id: nodes.parent_node_id, is_root: nodes.is_root, is_leaf: nodes.is_leaf, level_number: (parent_nodes.level_number + 1)}) AS node_json_path
FROM (SELECT product_nodes.node_id AS node_id, product_nodes.node_natural_key AS node_natural_key, product_nodes.node_name AS node_name, product_nodes.level_name AS level_name, product_nodes.parent_node_id AS parent_node_id, CASE WHEN (product_nodes.parent_node_id IS NULL) THEN true ELSE false END AS is_root, CASE WHEN (product_nodes.node_id IN (SELECT product_nodes.parent_node_id
FROM product_nodes)) THEN false ELSE true END AS is_leaf
FROM product_nodes) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
node_sort_order_query AS
(SELECT parent_nodes.node_id AS node_id, parent_nodes.node_natural_key AS node_natural_key, parent_nodes.node_name AS node_name, parent_nodes.level_name AS level_name, parent_nodes.parent_node_id AS parent_node_id, parent_nodes.is_root AS is_root, parent_nodes.is_leaf AS is_leaf, parent_nodes.level_number AS level_number, parent_nodes.node_json AS node_json, parent_nodes.node_json_path AS node_json_path, row_number() OVER (ORDER BY replace(CAST(parent_nodes.node_json_path AS VARCHAR), ']', '') ASC) AS node_sort_order
FROM parent_nodes)
 SELECT node_sort_order_query.node_id, node_sort_order_query.node_natural_key, node_sort_order_query.node_name, node_sort_order_query.level_name, node_sort_order_query.parent_node_id, node_sort_order_query.is_root, node_sort_order_query.is_leaf, node_sort_order_query.level_number, node_sort_order_query.node_sort_order, {node_id: node_id, node_natural_key: node_natural_key, node_name: node_name, level_name: level_name, parent_node_id: parent_node_id, is_root: is_root, is_leaf: is_leaf, level_number: level_number, node_sort_order: node_sort_order} AS node_json, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_id') AS level_1_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_natural_key') AS level_1_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'node_name') AS level_1_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_name') AS level_1_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'parent_node_id') AS level_1_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 1), 'level_number') AS level_1_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_id') AS level_2_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_natural_key') AS level_2_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'node_name') AS level_2_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_name') AS level_2_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'parent_node_id') AS level_2_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 2), 'level_number') AS level_2_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_id') AS level_3_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_natural_key') AS level_3_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'node_name') AS level_3_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_name') AS level_3_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'parent_node_id') AS level_3_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 3), 'level_number') AS level_3_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_id') AS level_4_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_natural_key') AS level_4_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'node_name') AS level_4_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_name') AS level_4_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'parent_node_id') AS level_4_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 4), 'level_number') AS level_4_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_id') AS level_5_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_natural_key') AS level_5_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'node_name') AS level_5_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_name') AS level_5_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'parent_node_id') AS level_5_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 5), 'level_number') AS level_5_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_id') AS level_6_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_natural_key') AS level_6_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'node_name') AS level_6_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_name') AS level_6_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'parent_node_id') AS level_6_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 6), 'level_number') AS level_6_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_id') AS level_7_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_natural_key') AS level_7_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'node_name') AS level_7_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_name') AS level_7_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'parent_node_id') AS level_7_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 7), 'level_number') AS level_7_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_id') AS level_8_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_natural_key') AS level_8_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'node_name') AS level_8_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_name') AS level_8_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'parent_node_id') AS level_8_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 8), 'level_number') AS level_8_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_id') AS level_9_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_natural_key') AS level_9_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'node_name') AS level_9_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_name') AS level_9_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'parent_node_id') AS level_9_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 9), 'level_number') AS level_9_level_number, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_id') AS level_10_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_natural_key') AS level_10_node_natural_key, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'node_name') AS level_10_node_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_name') AS level_10_level_name, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'parent_node_id') AS level_10_parent_node_id, struct_extract(list_extract(node_sort_order_query.node_json_path, 10), 'level_number') AS level_10_level_number
FROM node_sort_order_query
;

CREATE OR REPLACE TABLE product_aggregation_dim AS WITH RECURSIVE parent_nodes(node_id, node_natural_key, node_name, level_name, parent_node_id, is_root, is_leaf, level_number, node_sort_order, node_json, node_json_path) AS
(SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, [nodes.node_json] AS node_json_path
FROM (SELECT product_reporting_dim.node_id AS node_id, product_reporting_dim.node_natural_key AS node_natural_key, product_reporting_dim.node_name AS node_name, product_reporting_dim.level_name AS level_name, product_reporting_dim.parent_node_id AS parent_node_id, product_reporting_dim.is_root AS is_root, product_reporting_dim.is_leaf AS is_leaf, product_reporting_dim.level_number AS level_number, product_reporting_dim.node_sort_order AS node_sort_order, product_reporting_dim.node_json AS node_json, product_reporting_dim.level_1_node_id AS level_1_node_id, product_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, product_reporting_dim.level_1_node_name AS level_1_node_name, product_reporting_dim.level_1_level_name AS level_1_level_name, product_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, product_reporting_dim.level_1_level_number AS level_1_level_number, product_reporting_dim.level_2_node_id AS level_2_node_id, product_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, product_reporting_dim.level_2_node_name AS level_2_node_name, product_reporting_dim.level_2_level_name AS level_2_level_name, product_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, product_reporting_dim.level_2_level_number AS level_2_level_number, product_reporting_dim.level_3_node_id AS level_3_node_id, product_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, product_reporting_dim.level_3_node_name AS level_3_node_name, product_reporting_dim.level_3_level_name AS level_3_level_name, product_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, product_reporting_dim.level_3_level_number AS level_3_level_number, product_reporting_dim.level_4_node_id AS level_4_node_id, product_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, product_reporting_dim.level_4_node_name AS level_4_node_name, product_reporting_dim.level_4_level_name AS level_4_level_name, product_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, product_reporting_dim.level_4_level_number AS level_4_level_number, product_reporting_dim.level_5_node_id AS level_5_node_id, product_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, product_reporting_dim.level_5_node_name AS level_5_node_name, product_reporting_dim.level_5_level_name AS level_5_level_name, product_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, product_reporting_dim.level_5_level_number AS level_5_level_number, product_reporting_dim.level_6_node_id AS level_6_node_id, product_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, product_reporting_dim.level_6_node_name AS level_6_node_name, product_reporting_dim.level_6_level_name AS level_6_level_name, product_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, product_reporting_dim.level_6_level_number AS level_6_level_number, product_reporting_dim.level_7_node_id AS level_7_node_id, product_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, product_reporting_dim.level_7_node_name AS level_7_node_name, product_reporting_dim.level_7_level_name AS level_7_level_name, product_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, product_reporting_dim.level_7_level_number AS level_7_level_number, product_reporting_dim.level_8_node_id AS level_8_node_id, product_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, product_reporting_dim.level_8_node_name AS level_8_node_name, product_reporting_dim.level_8_level_name AS level_8_level_name, product_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, product_reporting_dim.level_8_level_number AS level_8_level_number, product_reporting_dim.level_9_node_id AS level_9_node_id, product_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, product_reporting_dim.level_9_node_name AS level_9_node_name, product_reporting_dim.level_9_level_name AS level_9_level_name, product_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, product_reporting_dim.level_9_level_number AS level_9_level_number, product_reporting_dim.level_10_node_id AS level_10_node_id, product_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, product_reporting_dim.level_10_node_name AS level_10_node_name, product_reporting_dim.level_10_level_name AS level_10_level_name, product_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, product_reporting_dim.level_10_level_number AS level_10_level_number
FROM product_reporting_dim) AS nodes UNION ALL SELECT nodes.node_id AS node_id, nodes.node_natural_key AS node_natural_key, nodes.node_name AS node_name, nodes.level_name AS level_name, nodes.parent_node_id AS parent_node_id, nodes.is_root AS is_root, nodes.is_leaf AS is_leaf, nodes.level_number AS level_number, nodes.node_sort_order AS node_sort_order, nodes.node_json AS node_json, array_append(parent_nodes.node_json_path, nodes.node_json) AS node_json_path
FROM (SELECT product_reporting_dim.node_id AS node_id, product_reporting_dim.node_natural_key AS node_natural_key, product_reporting_dim.node_name AS node_name, product_reporting_dim.level_name AS level_name, product_reporting_dim.parent_node_id AS parent_node_id, product_reporting_dim.is_root AS is_root, product_reporting_dim.is_leaf AS is_leaf, product_reporting_dim.level_number AS level_number, product_reporting_dim.node_sort_order AS node_sort_order, product_reporting_dim.node_json AS node_json, product_reporting_dim.level_1_node_id AS level_1_node_id, product_reporting_dim.level_1_node_natural_key AS level_1_node_natural_key, product_reporting_dim.level_1_node_name AS level_1_node_name, product_reporting_dim.level_1_level_name AS level_1_level_name, product_reporting_dim.level_1_parent_node_id AS level_1_parent_node_id, product_reporting_dim.level_1_level_number AS level_1_level_number, product_reporting_dim.level_2_node_id AS level_2_node_id, product_reporting_dim.level_2_node_natural_key AS level_2_node_natural_key, product_reporting_dim.level_2_node_name AS level_2_node_name, product_reporting_dim.level_2_level_name AS level_2_level_name, product_reporting_dim.level_2_parent_node_id AS level_2_parent_node_id, product_reporting_dim.level_2_level_number AS level_2_level_number, product_reporting_dim.level_3_node_id AS level_3_node_id, product_reporting_dim.level_3_node_natural_key AS level_3_node_natural_key, product_reporting_dim.level_3_node_name AS level_3_node_name, product_reporting_dim.level_3_level_name AS level_3_level_name, product_reporting_dim.level_3_parent_node_id AS level_3_parent_node_id, product_reporting_dim.level_3_level_number AS level_3_level_number, product_reporting_dim.level_4_node_id AS level_4_node_id, product_reporting_dim.level_4_node_natural_key AS level_4_node_natural_key, product_reporting_dim.level_4_node_name AS level_4_node_name, product_reporting_dim.level_4_level_name AS level_4_level_name, product_reporting_dim.level_4_parent_node_id AS level_4_parent_node_id, product_reporting_dim.level_4_level_number AS level_4_level_number, product_reporting_dim.level_5_node_id AS level_5_node_id, product_reporting_dim.level_5_node_natural_key AS level_5_node_natural_key, product_reporting_dim.level_5_node_name AS level_5_node_name, product_reporting_dim.level_5_level_name AS level_5_level_name, product_reporting_dim.level_5_parent_node_id AS level_5_parent_node_id, product_reporting_dim.level_5_level_number AS level_5_level_number, product_reporting_dim.level_6_node_id AS level_6_node_id, product_reporting_dim.level_6_node_natural_key AS level_6_node_natural_key, product_reporting_dim.level_6_node_name AS level_6_node_name, product_reporting_dim.level_6_level_name AS level_6_level_name, product_reporting_dim.level_6_parent_node_id AS level_6_parent_node_id, product_reporting_dim.level_6_level_number AS level_6_level_number, product_reporting_dim.level_7_node_id AS level_7_node_id, product_reporting_dim.level_7_node_natural_key AS level_7_node_natural_key, product_reporting_dim.level_7_node_name AS level_7_node_name, product_reporting_dim.level_7_level_name AS level_7_level_name, product_reporting_dim.level_7_parent_node_id AS level_7_parent_node_id, product_reporting_dim.level_7_level_number AS level_7_level_number, product_reporting_dim.level_8_node_id AS level_8_node_id, product_reporting_dim.level_8_node_natural_key AS level_8_node_natural_key, product_reporting_dim.level_8_node_name AS level_8_node_name, product_reporting_dim.level_8_level_name AS level_8_level_name, product_reporting_dim.level_8_parent_node_id AS level_8_parent_node_id, product_reporting_dim.level_8_level_number AS level_8_level_number, product_reporting_dim.level_9_node_id AS level_9_node_id, product_reporting_dim.level_9_node_natural_key AS level_9_node_natural_key, product_reporting_dim.level_9_node_name AS level_9_node_name, product_reporting_dim.level_9_level_name AS level_9_level_name, product_reporting_dim.level_9_parent_node_id AS level_9_parent_node_id, product_reporting_dim.level_9_level_number AS level_9_level_number, product_reporting_dim.level_10_node_id AS level_10_node_id, product_reporting_dim.level_10_node_natural_key AS level_10_node_natural_key, product_reporting_dim.level_10_node_name AS level_10_node_name, product_reporting_dim.level_10_level_name AS level_10_level_name, product_reporting_dim.level_10_parent_node_id AS level_10_parent_node_id, product_reporting_dim.level_10_level_number AS level_10_level_number
FROM product_reporting_dim) AS nodes, parent_nodes
WHERE nodes.parent_node_id = parent_nodes.node_id),
anon_1 AS
(SELECT struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_id') AS ancestor_node_id, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_natural_key') AS ancestor_node_natural_key, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_name') AS ancestor_node_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_name') AS ancestor_level_name, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_root') AS ancestor_is_root, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'is_leaf') AS ancestor_is_leaf, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'level_number') AS ancestor_level_number, struct_extract(list_extract(parent_nodes.node_json_path, 1), 'node_sort_order') AS ancestor_node_sort_order, parent_nodes.node_id AS descendant_node_id, parent_nodes.node_natural_key AS descendant_node_natural_key, parent_nodes.node_name AS descendant_node_name, parent_nodes.level_name AS descendant_level_name, parent_nodes.is_root AS descendant_is_root, parent_nodes.is_leaf AS descendant_is_leaf, parent_nodes.level_number AS descendant_level_number, parent_nodes.node_sort_order AS descendant_node_sort_order
FROM parent_nodes)
 SELECT anon_1.ancestor_node_id, anon_1.ancestor_node_natural_key, anon_1.ancestor_node_name, anon_1.ancestor_level_name, anon_1.ancestor_is_root, anon_1.ancestor_is_leaf, anon_1.ancestor_level_number, anon_1.ancestor_node_sort_order, anon_1.descendant_node_id, anon_1.descendant_node_natural_key, anon_1.descendant_node_name, anon_1.descendant_level_name, anon_1.descendant_is_root, anon_1.descendant_is_leaf, anon_1.descendant_level_number, anon_1.descendant_node_sort_order, anon_1.descendant_level_number - anon_1.ancestor_level_number AS net_level
FROM anon_1
;
