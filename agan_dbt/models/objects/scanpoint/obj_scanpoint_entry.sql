WITH entry AS (

  SELECT
      id
    , offer_id
    , device_id
    , category_id
    , bo_name
    , real_name
    , created_at
    , updated_at
    , scan_in
    , scan_out
    , scanpoint
    , department
    , COALESCE(normalized=1, FALSE) AS is_normalized

  FROM {{ source('scanpoint', 'entry') }}

)

SELECT * FROM entry
