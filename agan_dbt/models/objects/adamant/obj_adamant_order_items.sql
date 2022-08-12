WITH order_items AS (

  SELECT
      id AS order_item_id
    , order_id
    , tax
    , quantity
    , sku
    , price
    , name AS order_item_name

  FROM {{ ref ("obj_adamant.order_item_cleaned") }}

)

SELECT * FROM order_items