MODEL (
  name sqlmesh_example.values_demo,
  kind FULL,
  cron '@daily',
  grain id,
  audits (
    assert_salary_non_negative,
    not_null(columns := (id, name)),
    unique_values(columns := (id))
  )
);

SELECT
  t.id,
  t.name,
  t.salary
FROM (VALUES
  (1, 'Alice', 5000),
--   (2, 'Bob', -100),
  (3, 'Carol', 3000)
) AS t(id, name, salary)