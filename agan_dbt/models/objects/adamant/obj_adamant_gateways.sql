WITH gateway AS (

  SELECT
      id AS gateway_id
    , name AS gateway_name

  FROM {{ ref ("obj_adamant.gateway_cleaned") }}

)

SELECT * FROM gateway