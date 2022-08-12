WITH unioned AS (
  SELECT
    product_check_id
    , product_check_field_id
    , clarification
    , defect
    , value
  FROM {{ ref('obj_wkfs.product_check_value_cleaned') }}

)
SELECT
  pcv.product_check_id
  , pcv.product_check_field_id
  , COALESCE(pcv.clarification = 1, false) AS is_clarification
  , COALESCE(pcv.defect = 1, false) AS is_defect
  , pcv.value
  , pcf.field_type
  -- max product_check_field_id at 2020-04-28 is 132, won't exceed 10k
  -- use a composite key as id for hooks to work
  , pcv.product_check_field_id + pcv.product_check_id * 10 ^ 4 AS id
FROM unioned AS pcv
  LEFT JOIN {{ ref('obj_wkfs.product_check_field_cleaned') }} AS pcf
    ON pcf.id = pcv.product_check_field_id