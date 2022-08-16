WITH order_item_details AS (

  SELECT * FROM {{ ref('obj_adamant_order_item_details') }}

)

SELECT * FROM order_item_details
