WITH top_customers AS (
SELECT
    c_w_id,
    c_d_id,
    c_id,
    c_first,
    c_middle,
    c_last,
    c_balance
FROM
    customer
ORDER BY
    c_balance DESC
LIMIT
    10
)

SELECT
    c.c_first,
    c.c_middle,
    c.c_last,
    c.c_balance,
    w.w_name,
    d.d_name
FROM
    top_customers c
    JOIN district d ON c.c_w_id = d.d_w_id
    AND c.c_d_id = d.d_id
    JOIN warehouse w ON d.d_w_id = w.w_id;