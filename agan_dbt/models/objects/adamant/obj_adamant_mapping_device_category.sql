-- This is a separate model due to n:n relationship
WITH device_category AS (

  SELECT
      dc.device_id
    , dc.category_id
    , c.category_name

  FROM {{ ref ("obj_adamant.device_category_cleaned") }} as dc
  LEFT JOIN {{ ref ("obj_adamant_categories") }} AS c
    ON c.category_id = dc.category_id

)

SELECT * FROM device_category