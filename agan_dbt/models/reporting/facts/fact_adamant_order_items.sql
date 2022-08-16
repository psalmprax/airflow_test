WITH order_items AS (

  SELECT * FROM {{ ref('obj_adamant_order_items') }}

)

SELECT * FROM order_items
