WITH sales_channel AS (

  SELECT
      id AS sales_channel_id
    , name AS sales_channel_name
    , COALESCE(warranty_allowed=1, FALSE) AS is_warranty_allowed
    , language

  FROM {{ ref ("obj_adamant.sales_channel_cleaned") }}

)

SELECT * FROM sales_channel