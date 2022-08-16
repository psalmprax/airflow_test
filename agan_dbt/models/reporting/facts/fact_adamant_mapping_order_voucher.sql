WITH mapping AS (

  SELECT * FROM {{ ref('obj_adamant_mapping_order_voucher') }}

)

SELECT * FROM mapping
