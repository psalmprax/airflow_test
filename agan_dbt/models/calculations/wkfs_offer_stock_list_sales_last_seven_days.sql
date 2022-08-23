--SELECT DISTINCT
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT AS id
--	, COUNT(o.id) AS offers_sold_last_seven_days
--FROM {{ ref ("obj_wkfs.stock_out_info_cleaned") }} AS soi
--    JOIN {{ ref ("obj_wkfs.offer_cleaned") }} AS o
--        ON soi.stock_id = o.latest_stock_id
--    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
--        ON d.id = CASE WHEN o.product_check_device_id IS NOT NULL THEN o.product_check_device_id ELSE o.device_id END
--    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
--        ON d.manufactor_id = m.id
--    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
--        ON m.category_id = c.id
--WHERE soi.created_at >= now() - INTERVAL '7 DAY'
--GROUP BY
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT
SELECT DISTINCT
    (c.id||m.id||d.id||CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END)::BIGINT AS id
	, COUNT(o.id) AS offers_sold_last_seven_days
FROM {{ ref ("obj_wkfs.stock_out_info_cleaned") }} AS soi
    JOIN {{ ref("obj_wkfs.offer_cleaned") }} AS o
        ON soi.stock_id = o.latest_stock_id
    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
        ON d.id = CASE WHEN o.product_check_device_id IS NOT NULL THEN o.product_check_device_id ELSE o.device_id END
    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
        ON d.manufactor_id = m.id
    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
        ON m.category_id = c.id
WHERE soi.created_at >= getdate() - INTERVAL '7 DAY'
GROUP BY
    (c.id||m.id||d.id||CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END)::BIGINT