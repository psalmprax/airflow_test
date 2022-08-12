WITH state AS (

  SELECT * FROM {{ ref('obj_wkfs_offer_state') }}

), offer_state_set AS (

  SELECT
      oss.id
    , oss.offer_id
    , oss.offer_state_id
    , oss.sf_guard_user_id
    , oss.created_at::TIMESTAMP WITH TIME ZONE AS created_at
    , s.offer_state

  FROM {{ ref('obj_wkfs.offer_offer_state_set_cleaned') }} AS oss
    LEFT JOIN state AS s
      ON s.id = oss.offer_state_id

)

SELECT * FROM offer_state_set