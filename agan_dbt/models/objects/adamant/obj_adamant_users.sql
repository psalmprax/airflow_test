WITH users AS (

  SELECT
      id AS user_id
    , username
    , username_canonical
    , email
    , email_canonical
    , COALESCE(enabled=1, FALSE) AS is_enabled
    , last_login AS last_login_at
    , created_at
    , updated_at
    , gender
    , firstname
    , lastname

  FROM {{ ref ("obj_adamant.fos_user_user_cleaned") }}

)

SELECT * FROM users