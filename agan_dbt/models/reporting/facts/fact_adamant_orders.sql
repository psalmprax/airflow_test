WITH orders AS (

  SELECT * FROM {{ ref('obj_adamant_orders') }}

)

SELECT * FROM orders
