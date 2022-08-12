WITH business_channel AS (

  SELECT
      id AS business_channel_id
    , name AS business_channel_name
    , back_office_id
  FROM {{ ref ("obj_adamant.business_channel_cleaned") }}  
)

SELECT * FROM business_channel