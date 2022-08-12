WITH wkfs_bch AS (

SELECT 
  id
  , sf_guard_user_id 
  , business_channel_type_id 
  , outlet_name AS business_channel_name
  , deleted
 
FROM {{ ref('obj_wkfs.business_channel_cleaned') }}

)
, wkfs_bch_type AS (
  SELECT 
     bc.*
     , bct."name" AS business_channel_type
  FROM wkfs_bch AS bc 
  LEFT JOIN {{ ref('obj_wkfs.business_channel_type_cleaned') }} bct 
    ON bc.business_channel_type_id = bct.id
)

SELECT 
  id
  , business_channel_name
  , business_channel_type_id 
  , business_channel_type
  , deleted
FROM wkfs_bch_type