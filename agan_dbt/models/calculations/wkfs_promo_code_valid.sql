SELECT
    o.id
 ,  COALESCE(sum(o.accepted_amount), 0) AS accepted_amount
 ,  COALESCE(sum(o.converted_amount), 0) AS converted_amount -- is needed for applications with other currencies
 ,  COALESCE(sum(po.promo_amount), 0) AS promo_amount
 ,  COALESCE(sum(pc.min_amount_value), 0) AS min_amount_value
 ,  CASE WHEN
        SUM(o.converted_amount)>0
    THEN
        CASE WHEN SUM(o.converted_amount)>SUM(COALESCE(pc.min_amount_value, 0)) THEN 1 ELSE 0 END
    ELSE
        CASE WHEN SUM(o.accepted_amount)>SUM(COALESCE(pc.min_amount_value, 0))  THEN 1 ELSE 0 END
    END AS is_promo_code_valid -- checks if promo requirement is valid
FROM {{ ref ("obj_wkfs.offer_cleaned") }} AS o
LEFT JOIN {{ ref ("obj_wkfs.promo_offer_cleaned") }} po
    ON o.id = po.offer_id
LEFT JOIN {{ ref ("obj_wkfs.promo_code_cleaned") }}  pc
    ON pc.id = po.promo_id
GROUP BY o.id