--SELECT DISTINCT
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT as id
--	, COUNT(o.id) AS stock
--	, SUM(o.accepted_amount) AS ek
--	, TO_CHAR(sum(now() - s.stock_in) / COUNT(o.id), 'dd')::BIGINT AS offers_stock_time
--FROM {{ ref ("obj_wkfs.offer_cleaned") }} AS o
--    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
--        ON d.id = CASE WHEN o.product_check_device_id IS NOT NULL THEN o.product_check_device_id ELSE o.device_id END
--    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
--        ON d.manufactor_id = m.id
--    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
--        ON m.category_id = c.id
--    JOIN {{ ref("obj_wkfs.offer_offer_state_cleaned") }} AS oos
--        ON o.id = oos.offer_id
--    JOIN {{ ref("obj_wkfs.stock_cleaned") }} s
--        ON s.id = o.latest_stock_id
--WHERE oos.offer_state_id IN (19, 22)
--GROUP BY
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT
SELECT DISTINCT
    (c.id||m.id||d.id||CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END)::BIGINT AS id
	, COUNT(o.id) AS stock
	, SUM(o.accepted_amount) AS ek
    , (sum(getdate()::date - s.stock_in::date) /COUNT(o.id))::BIGINT AS offers_stock_time

FROM {{ ref("obj_wkfs.offer_cleaned") }} AS o
    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
        ON d.id = CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
        ON d.manufactor_id = m.id
    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
        ON m.category_id = c.id
    JOIN {{ ref("obj_wkfs.offer_offer_state_cleaned") }} AS oos
        ON o.id = oos.offer_id
    JOIN {{ ref("obj_wkfs.stock_cleaned") }} s
        ON s.id = o.latest_stock_id
WHERE oos.offer_state_id IN (19, 22)
GROUP BY
    (c.id||m.id||d.id||CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END)::BIGINT
--    , s.stock_in
--    , c.id, m.id, d.id