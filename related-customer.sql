WITH customerItem AS (
    SELECT
        c.c_id,
        c.c_w_id,
        c.c_d_id,
        ol.ol_i_id as i_id
    FROM
        customer c
        JOIN "order" o ON c.c_id = o.o_c_id
        JOIN orderLine ol ON o.o_id = ol.ol_o_id
)

SELECT
    ci1.c_w_id,
    ci1.c_d_id,
    ci1.c_id,
    count(distinct(ci1.i_id, ci2.i_id))
FROM
    customerItem ci1
    JOIN customerItem ci2 ON ci1.i_id = ci2.i_id
WHERE
    ci1.c_w_id <> %(input_warehouse_id)s
    AND ci2.c_id = %(input_customer_id)s
GROUP BY ci1.c_id, ci1.c_w_id, ci1.c_d_id
HAVING count(distinct(ci1.i_id, ci2.i_id)) >= 2