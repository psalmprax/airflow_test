SELECT
  ar.id
  , ar.offer_id
  , q.calculation_code
  , q.question
  , q.description
  , q.deprication
  , COALESCE(q.deprication_answer = 1, FALSE) AS is_deprication_answer
  , COALESCE(q.accessory = 1, FALSE) AS is_accessory
  , q.accessory_text
  , COALESCE(q.device_question = 1, FALSE) AS is_device_question
  , COALESCE(q.visible_in_frontend = 1,FALSE) AS is_question_visible_in_frontend
  , qa.answer_text
  , qa.answer_code
  , COALESCE(qa.answer_is_title = 1, FALSE) AS is_answer_is_title
  , COALESCE(qa.visible_in_frontend = 1, FALSE) AS is_answer_visible_in_frontend
  , qa.visible_in_frontend
FROM {{ ref('obj_wkfs.question_cleaned') }} AS q
  LEFT JOIN {{ ref('obj_wkfs.answer_cleaned') }} AS ar
    ON ar.question_id = q.id
  LEFT JOIN {{ ref('obj_wkfs.question_answer_cleaned') }} AS qa
    ON qa.id = ar.question_answer_id
WHERE NOT ar.id IS NULL -- sort out questions that were never answered