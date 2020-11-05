SET TRANSACTION AS OF SYSTEM TIME %(current_timestamp)s;
WITH customerItem AS (
    SELECT
        c.c_id,
        c.c_w_id,
        c.c_d_id,
        o.o_id,
        ol.ol_i_id as i_id
    FROM
        customer c
        JOIN "order" o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
        JOIN orderline ol ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
        WHERE c.c_w_id = %(input_warehouse_id)s AND c.c_d_id = %(input_district_id)s AND c.c_id = %(input_customer_id)s
)

SELECT DISTINCT
    c2.c_w_id,
    c2.c_d_id,
    c2.c_id
FROM
    customerItem ci1
    JOIN customer c2 ON c2.c_w_id <> ci1.c_w_id
    JOIN "order" o ON c2.c_w_id = o.o_w_id AND c2.c_d_id = o.o_d_id AND c2.c_id = o.o_c_id
    JOIN orderline ol ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
    WHERE ol.ol_i_id = ci1.i_id
    GROUP BY ci1.c_w_id, ci1.c_d_id, ci1.o_id, ci1.c_id, c2.c_w_id, c2.c_d_id, o.o_id, c2.c_id
    HAVING count(distinct(ci1.i_id, ol.ol_i_id)) >= 2;