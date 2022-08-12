-- This is a separate model due to n:n relationship
WITH order_voucher AS (

  SELECT
      ov.order_id
    , ov.voucher_id
    , v.voucher_amount
    , v.voucher_code

  FROM {{ ref ("obj_adamant.order_voucher_cleaned") }} as ov
    LEFT JOIN {{ ref ("obj_adamant_vouchers") }}  AS v
      ON v.voucher_id = ov.voucher_id

)

SELECT * FROM order_voucher