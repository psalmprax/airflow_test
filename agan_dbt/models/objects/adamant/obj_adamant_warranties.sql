WITH warranty AS (

  SELECT
      id AS warranty_id
    , started_at
    , finished_at
    , duration
    , created_at
    , updated_at
    , COALESCE(hide_notification_email=1, FALSE) AS hide_notification_email

  FROM {{ ref ("obj_adamant.warranty_cleaned") }} AS c

)

SELECT * FROM warranty