-- This is a separate model due to n:n relationship
WITH mapping AS (

  SELECT * FROM {{ ref('obj_adamant_mapping_device_category') }}

)

SELECT * FROM mapping
