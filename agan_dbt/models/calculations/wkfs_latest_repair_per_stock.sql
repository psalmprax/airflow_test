SELECT
  pc.stock_id
  , MAX(r.check_result_actions_id) AS wkfs_latest_repair_per_stock
FROM {{ ref ("obj_wkfs.repair_cleaned") }} AS r
  LEFT JOIN {{ ref ("obj_wkfs.check_result_action_cleaned") }} AS cra
    ON cra.id = r.check_result_actions_id
  LEFT JOIN {{ ref ("obj_wkfs_product_check") }} AS pc
    ON pc.id = cra.product_checks_id_in
GROUP BY 1