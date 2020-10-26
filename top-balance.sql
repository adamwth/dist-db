SELECT
    c.c_first,
    c.c_middle,
    c.c_last,
    c.c_balance,
    w.w_name,
    d.d_name
FROM
    customer c
    JOIN district d ON c.c_w_id = d.d_w_id
    AND c.c_d_id = d.d_id
    JOIN warehouse w ON d.d_w_id = w.w_id
ORDER BY
    c.c_balance DESC
LIMIT
    10;