SELECT
  offer_id
  , MAX(created_at) AS last_changed_at
FROM {{ ref ("obj_wkfs.offer_offer_state_set_cleaned") }}
GROUP BY 1