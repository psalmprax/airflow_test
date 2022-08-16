/*
 * User sensitive information is only available in the object (base) models,
 * with restricted access.
 */
WITH users AS (

  SELECT
      user_id
    , is_enabled
    , last_login_at
    , created_at
    , updated_at

   FROM {{ ref('obj_adamant_users') }}

)

SELECT * FROM users
