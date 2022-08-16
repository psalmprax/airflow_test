WITH tracking AS (

  SELECT
      tr.id
    , tr.employee_id
    , em.bo_name AS employee_back_office_name
    , em.real_name AS employee_real_name
    , tr.scanpoint_id
    , sc.name AS scanpoint_name
    , tr.department_id
    , dp.department_name
    , tr.offer_id
    , o.device_id
    , o.condition_id
    , o.condition_title
    , o.manufactor_id
    , o.manufactor_name
    , o.deleted
    , o.category_id
    , o.category AS category_name
    , tr.timestamp_in
    , tr.timestamp_out
    , convert(timestamp,case tr.timestamp_in when '' then null else tr.timestamp_in end) as started_on
    , convert(timestamp,case tr.timestamp_out when '' then null else tr.timestamp_out end) as completed_on
--    , convert(DATE, case tr.timestamp_out when '' then null else tr.timestamp_out end)
--      - convert(DATE, case tr.timestamp_in when '' then null else tr.timestamp_in end) AS working_time
  FROM {{ source('scanpoint', 'tracking') }} AS tr
    LEFT JOIN {{ ref('obj_scanpoint_scanpoint') }} AS sc
      ON sc.id = tr.scanpoint_id
    LEFT JOIN {{ ref('obj_scanpoint_department') }} AS dp
      ON dp.id = tr.department_id
    LEFT JOIN {{ ref('obj_scanpoint_employee') }} AS em
      ON em.id = tr.employee_id
    LEFT JOIN {{ ref('obj_wkfs_offer') }} AS o
      ON o.id::TEXT = tr.offer_id

)

SELECT * FROM tracking

