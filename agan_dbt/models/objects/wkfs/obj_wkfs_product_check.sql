WITH unioned AS (
  SELECT
    id
    , stock_id
    , checked
    , state
    , sf_guard_user_id
    , warranty
    , costs_estimate
    , costs_estimate_optical
    , serial
    , ovp
    , color
    , color_id
    , additional_accessories
    , ebay_product_id
    , "time"
    , video
    , box AS boxnumber
    , imei
    , quality_control
    , optical_state
  FROM {{ ref('obj_wkfs.product_check_cleaned') }}

),
clarifications AS (
  SELECT
    product_check_id
    , count(*) AS counter_total
    , COUNT(CASE WHEN is_defect THEN 1 END) AS counter_defects
    , COUNT(CASE WHEN field_type = 1 THEN 1 END) AS counter_technical
    , COUNT(CASE WHEN field_type = 2 THEN 1 END) AS counter_optical
    , COUNT(CASE WHEN field_type = 3 THEN 1 END) AS counter_general_data
  FROM {{ ref('obj_wkfs_product_check_value') }}
  WHERE is_clarification
    AND field_type IN (1, 2, 3)
    /*
     *  field types:
     *    1 = technical
     *    2 = optical
     *    3 = general
     */
  GROUP BY 1
),
clarifications_questionair AS (
  SELECT
    product_check_id
    , SUM(CASE WHEN is_clarify THEN 1 ELSE 0 END) AS counter_clarify
  FROM {{ ref('obj_wkfs_product_check_questionair') }}
  GROUP BY 1
),
raws AS (
  SELECT
    pc.*
    , pc.ovp = 1 AS is_ovp
    , COALESCE(cs.counter_technical, 0) > 0 AS is_clarification_technical
    , COALESCE(cs.counter_optical, 0) > 0 AS is_clarification_optical
    , COALESCE(cs.counter_general_data, 0) > 0 AS is_clarification_general_data
    , COALESCE(cs.counter_defects, 0) > 0 AS is_defect
    , COALESCE(cq.counter_clarify, 0) > 0 AS is_clarification_questionnaire
    , COALESCE(cs.counter_total, 0) > 0 OR COALESCE(cq.counter_clarify, 0) > 0
      AS is_clarify
  FROM unioned AS pc
    LEFT JOIN clarifications AS cs
      ON cs.product_check_id = pc.id
    LEFT JOIN clarifications_questionair AS cq
      ON cq.product_check_id = pc.id
)
SELECT
  raws.*
  , CASE
    WHEN raws.is_clarify
      THEN ROW_NUMBER() OVER (
        PARTITION BY raws.stock_id, raws.is_clarify
        ORDER BY raws.id DESC
      )
    END AS order_of_clarify_checks_per_stock_desc
    /* stocks need to know the latest product check with clarification */
  , CASE
      WHEN raws.is_defect THEN 'defect'
      ELSE 'passed'
    END AS status
FROM raws
