SELECT 
  id
  , business_channel_name
  , business_channel_type_id 
  , business_channel_type
  , deleted
FROM {{ ref('obj_wkfs_business_channels')}}
