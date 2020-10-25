WITH customerItem AS (
    SELECT
        c.id,
        c.w_id,
        c.d_id,
        ol.ol_i_id as i_id
    FROM
        customer c
        JOIN "order" o ON c.id = o.o_c_id
        JOIN orderLine ol ON o.id = ol.ol_o_id
);

SELECT
    c.c_w_id,
    c.c_d_id,
    c.c_id
FROM
    customer c
WHERE
    c.c_w_id <> %(input_warehouse_id)s
    AND count(
        distinct(
            SELECT
                *
            FROM
                customerItem ci1
                JOIN customerItem ci2 ON ci1.i_id = ci2.i_id
            WHERE
                ci1.c_id = c.c_w_id
                AND ci2.c_id = %(input_customer_id)s
        )
    ) >= 2;