WITH category AS (

  SELECT
      id AS category_id
    , name AS category_name

  FROM {{ ref ("obj_adamant.category_cleaned") }}

)

SELECT * FROM category