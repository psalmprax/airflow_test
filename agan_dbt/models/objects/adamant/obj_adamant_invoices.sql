WITH invoice AS (

  SELECT
      id AS invoice_id
    , created_at
    , updated_at

  FROM {{ ref ("obj_adamant.invoice_cleaned") }}  

)

SELECT * FROM invoice