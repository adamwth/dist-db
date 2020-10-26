WITH lastLOrderIds AS (
    SELECT
        *
    FROM
        district d
        JOIN order o ON d.d_w_id = o.o_w_id
        AND d.d_id = o.o_d_id
    WHERE
        o.o_id < d.d_next_o_id
    ORDER BY
        o.o_id DESC
    LIMIT
        %(input_num_last_orders) s
), lastLOrders as (
    SELECT
        *
    FROM
        lastLOrderIds llid
        JOIN orderLine ol ON llid.o_id = ol.ol_o_id
    WHERE
        ol.ol_d_id = %(input_warehouse_id) s
        AND ol.ol_w_id = %(input_district_id) s
),
SELECT
    ol_o_id,
    o_entry_d,
    c_first,
    c_middle,
    c_last,
    i_name,
    ol_quantity
FROM
    lastLOrders
    JOIN customer ON o_w_id = c_w_id
    AND o_d_id = c_d_id
    AND o_c_id = c_id
    JOIN item ON ol_i_id = i_id
WHERE
    (ol_o_id, ol_quantity) in (
        SELECT
            ol_o_id,
            max(ol_quantity)
        FROM
            lastLOrders
        GROUP BY
            ol_o_id
    )
GROUP BY
    ol_o_id