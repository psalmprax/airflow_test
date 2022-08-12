WITH raws AS (
  SELECT
    product_check_id
    , question_id
    , clarify
    , question_answer_id
  FROM {{ ref('obj_wkfs.product_check_questionair_cleaned') }}

)
SELECT
  product_check_id
  , question_id
  , question_answer_id
  , clarify = 1 AS is_clarify
  , question_id + product_check_id * 10 ^ 4 AS id
FROM raws