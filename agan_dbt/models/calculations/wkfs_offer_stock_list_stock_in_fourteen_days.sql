--SELECT DISTINCT
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT AS id
--	, COUNT(o.id) AS offers_stock_in_fourteen_days
--FROM {{ ref ("obj_wkfs.offer_cleaned") }} AS o
--    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
--        ON d.id = CASE WHEN o.product_check_device_id IS NOT NULL THEN o.product_check_device_id ELSE o.device_id END
--    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
--        ON d.manufactor_id = m.id
--    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
--        ON m.category_id = c.id
--WHERE o.created_at >= now() - INTERVAL '14 DAY' AND o.latest_stock_id IS NOT NULL
--GROUP BY
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT
SELECT DISTINCT
    c.id||m.id||d.id||o.condition::BIGINT AS id
	, COUNT(o.id) AS offers_stock_in_fourteen_days
FROM ( select id, latest_stock_id, created_at, CASE WHEN product_check_device_condition IS NULL THEN 5 ELSE product_check_device_condition end  AS condition,
		CASE WHEN product_check_device_id IS NOT NULL THEN product_check_device_id ELSE device_id END device_id
	  from analytics_objects."obj_wkfs.offer_cleaned") AS o
    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
        ON d.id = o.device_id
    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
        ON d.manufactor_id = m.id
    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
        ON m.category_id = c.id
WHERE o.created_at >= getdate() - INTERVAL '14 DAY' AND o.latest_stock_id IS NOT NULL
GROUP BY
    c.id||m.id||d.id||o.condition::BIGINT