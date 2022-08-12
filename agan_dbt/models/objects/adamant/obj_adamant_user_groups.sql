WITH fos_user_group AS (

  SELECT
      id AS user_group_id
    , name AS user_group_name

--  FROM raw_adamant.fos_user_group
  FROM {{ ref ("obj_adamant.fos_user_group_cleaned") }}

)

SELECT * FROM fos_user_group