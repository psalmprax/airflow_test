
WITH product_check_value AS (
 SELECT 
    product_check_id AS id
    , value
 FROM {{ ref('obj_wkfs.product_check_value_cleaned') }}
 WHERE product_check_field_id = 15 AND value IN (SELECT id::text FROM {{ ref('obj_wkfs.device_condition_cleaned') }} )
)
, product_check AS (
    SELECT
        pchc.*
        , owpc.stock_id 
    FROM product_check_value AS pchc
    JOIN {{ ref('obj_wkfs.product_check_cleaned') }} AS owpc
      ON pchc.id = owpc.id
)
, last_product_check AS (
  
  -- a stock_id can have multiple product_check_id
  -- product_check_id is generated with an autoincremented key
  -- we take the greatest product_check_id
  SELECT * FROM 
    (SELECT 
       *
        ,rank() OVER (PARTITION BY stock_id ORDER BY id DESC) AS idx
--       ,rank() OVER w AS idx
     FROM product_check
--     WINDOW w AS (PARTITION BY stock_id ORDER BY id DESC)
     ) tmp
  WHERE idx = 1
)

SELECT 
* 
FROM 
last_product_check