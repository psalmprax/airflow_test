

SELECT
  stock_id
  , MAX(id) AS product_check_id
FROM {{ ref('obj_wkfs_product_check') }}
GROUP BY 1