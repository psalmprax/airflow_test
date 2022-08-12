WITH fos_user_group AS (

  SELECT
      uug.user_id
    , uug.group_id
    , ug.user_group_name

  FROM {{ ref ("obj_adamant.fos_user_user_group_cleaned") }} AS uug
    LEFT JOIN {{ ref ("obj_adamant_user_groups") }} AS ug
      ON ug.user_group_id = uug.group_id

)

SELECT * FROM fos_user_group