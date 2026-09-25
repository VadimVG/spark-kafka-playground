AUDIT (
  name assert_salary_non_negative
);

SELECT *
FROM @this_model
WHERE salary < 0