/*
 *  A product check have multiple check result actions. Get the latest one.
 */
WITH raws AS (
  SELECT
    cra.id
    , cra.product_checks_id_in
    , ROW_NUMBER() OVER (
      PARTITION BY cra.product_checks_id_in ORDER BY cra.id DESC
    ) AS rn
  FROM {{ ref('obj_wkfs.check_result_action_cleaned') }} AS cra
)
SELECT
  raws.product_checks_id_in
  , cl.*
  , COALESCE(cl.accepted = 1, FALSE) AS is_accepted
  , COALESCE(cl.closed = 1, FALSE) AS is_closed
  , COALESCE(cl.closed_confirm = 1, FALSE) AS is_closed_confirmed
  , COALESCE(cl.skipped = 1, FALSE) AS is_skipped
  , COALESCE(cl.submitted = 1, FALSE) AS is_submitted
FROM raws
  LEFT JOIN {{ ref('obj_wkfs.clarification_cleaned') }} AS cl

    ON raws.rn = 1
    AND cl.check_result_actions_id = raws.id
WHERE raws.rn = 1