WITH lastLOrderLines AS (
    SELECT
        *
    FROM
        district
        JOIN "order" ON d_w_id = o_w_id AND d_id = o_d_id
        JOIN orderLine ON o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id
    WHERE
        o_id < d_next_o_id
        AND ol_d_id = %(input_district_id)s
        AND ol_w_id = %(input_warehouse_id)s
        AND o_id >= d_next_o_id - %(input_num_last_orders)s
)
SELECT
    ol_o_id,
    o_entry_d,
    c_first,
    c_middle,
    c_last,
    i_name,
    ol_quantity
FROM
    lastLOrderLines
    JOIN customer ON o_w_id = c_w_id AND o_d_id = c_d_id AND o_c_id = c_id
    JOIN item ON ol_i_id = i_id
WHERE
    (ol_o_id, ol_quantity) in (
        SELECT
            ol_o_id,
            max(ol_quantity)
        FROM
            lastLOrderLines
        GROUP BY
            ol_o_id
    );