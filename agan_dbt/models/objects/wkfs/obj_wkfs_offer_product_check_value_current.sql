WITH stock AS (

  SELECT
      *
    , ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY created_at DESC) AS rn

  FROM {{ ref('obj_wkfs.stock_cleaned') }}

), last_product_check_per_stock AS (

  SELECT * FROM {{ ref('wkfs_last_product_check_per_stock') }}

), product_check_value AS (

  SELECT
      product_check_id
    , product_check_field_id
    , NULLIF(value, '') AS value
    , created_at
    , updated_at

  FROM {{ ref('obj_wkfs.product_check_value_cleaned') }}

), final AS (

  SELECT
--  /*
--   * This cte replaces a raw wkfs model and follows the same filtering logic.
--   * I.e.,
--   * - uses the last product check per stock
--   * - as well as the last stock value per offer
--   * - and does not include product checks with NULL values
--   *
--   */
--
--      CONCAT(convert(varchar(max), stock.offer_id)
--            ,convert(varchar(max), '-')
--            ,convert(varchar(max), pc.product_check_id)
--            ,convert(varchar(max), '-')
--            ,convert(varchar(max), pcv.product_check_field_id)
--            ) AS id
     convert(varchar(max), stock.offer_id) || convert(varchar(max), '-') || convert(varchar(max), pc.product_check_id)
     || convert(varchar(max), '-') || convert(varchar(max), pcv.product_check_field_id) AS id
    , stock.offer_id
    , pc.product_check_id
    , pcv.product_check_field_id
    , pcv.value
    , pcv.created_at
    , pcv.updated_at

  FROM last_product_check_per_stock AS pc
  	LEFT JOIN stock
  		ON stock.id = pc.stock_id
  	LEFT JOIN product_check_value AS pcv
  		ON pcv.product_check_id = pc.product_check_id
  WHERE NOT pcv.value IS NULL
  AND stock.rn = 1

)

SELECT * FROM final