
SELECT 
   business_channel_id AS id
  , business_channel_name
FROM {{ ref('obj_adamant_business_channel') }}
