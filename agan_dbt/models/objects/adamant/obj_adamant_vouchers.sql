WITH voucher AS (

  SELECT
      id AS voucher_id
    , code AS voucher_code
    , amount AS voucher_amount
--    , _ewah_executed_at

  FROM {{ ref ("obj_adamant.voucher_cleaned") }}

)

SELECT * FROM voucher

