
SELECT 
  sales_channel_id AS id 
  , sales_channel_name 
  , language

FROM {{ ref('obj_adamant_sales_channels') }} oasc 

