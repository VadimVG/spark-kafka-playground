MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
  tags (test)
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders,
  MIN(event_date) AS first_event_date,
FROM
  sqlmesh_example.incremental_model
GROUP BY item_id
  