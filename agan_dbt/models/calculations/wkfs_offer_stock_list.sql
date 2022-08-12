--SELECT DISTINCT
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT AS id
--    , c.name AS category
--    , m.name AS brand
--    , CONCAT(m.name,' ',d.model,' ',d.capacity) AS device
--    , CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--      AS condition
--FROM {{ ref ("obj_wkfs.offer_cleaned") }} AS o
--    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
--        ON d.id = CASE WHEN o.product_check_device_id IS NOT NULL THEN o.product_check_device_id ELSE o.device_id END
--    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
--        ON d.manufactor_id = m.id
--    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
--        ON m.category_id = c.id
--GROUP BY
--    CONCAT(
--        c.id,
--        m.id,
--        d.id,
--        CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--    )::BIGINT,
--    c.name,
--    m.name,
--    CONCAT(m.name,' ',d.model,' ',d.capacity),
--    CASE WHEN o.product_check_device_condition IS NULL THEN 5 ELSE o.product_check_device_condition END
--
SELECT DISTINCT
    c.id||m.id||d.id||o.condition::BIGINT AS id,
    c.name AS category
    , m.name AS brand
    , m.name||' '||d.model||' '||d.capacity AS device
    , o.condition
FROM ( select CASE WHEN product_check_device_condition IS NULL THEN 5 ELSE product_check_device_condition end  AS condition,
		CASE WHEN product_check_device_id IS NOT NULL THEN product_check_device_id ELSE device_id END device_id
	  from {{ ref ("obj_wkfs.offer_cleaned") }}) AS o
    JOIN {{ ref("obj_wkfs.device_cleaned") }} AS d
        ON d.id = o.device_id
    JOIN {{ ref ("obj_wkfs.manufactor_cleaned") }} AS m
        ON d.manufactor_id = m.id
    JOIN {{ ref("obj_wkfs.category_cleaned") }} AS c
        ON m.category_id = c.id
GROUP BY
    c.id||m.id||d.id||o.condition::BIGINT,
    c.name,
    m.name,
    m.name||' '||d.model||' '||d.capacity,
    o.condition