WITH shipping AS (

  SELECT
      id AS shipping_id
    , name AS shipping_name

  FROM {{ ref ("obj_adamant.shipping_cleaned") }}

)

SELECT * FROM shipping