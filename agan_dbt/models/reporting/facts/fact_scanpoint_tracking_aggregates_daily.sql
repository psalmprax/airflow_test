WITH tracking AS (

  SELECT 
     employee_id
    , employee_real_name
    , department_id
    , department_name
    --, deleted
    , CASE 
        WHEN deleted  IS NULL THEN 999999
        ELSE deleted
      END AS deleted
    , CASE 
        WHEN category_id IS NULL THEN 999999
        ELSE category_id
      END AS category_id
    , CASE 
        WHEN category IS NULL THEN 'empty'
        ELSE category
      END
      
    , CASE 
        WHEN condition_id IS NULL THEN 999999
        ELSE condition_id
      END AS condition_id 
    , started_on
    , completed_on 
    , offer_id 
  FROM {{ ref('fact_scanpoint_tracking') }}

), dates AS (

  SELECT date FROM {{ ref('dim_dates') }}

), employee_department AS (

  SELECT DISTINCT
      employee_id
    , employee_real_name
    , department_id
    , department_name
    , category_id
    , category
    , condition_id
    , deleted

  FROM tracking

), date_employee_department AS (

  -- cross join for every date and employee_department
  SELECT * FROM dates, employee_department

), tracking_started AS (

  SELECT
      started_on
    , employee_id
    , employee_real_name
    , department_id
    , department_name
    , category_id
    , category
    , condition_id
    , deleted
    , COUNT(offer_id) AS number_of_offers_started
  FROM tracking
  GROUP BY 1,2,3,4,5,6,7,8,9

), tracking_completed AS (

  SELECT
      completed_on
    , employee_id
    , employee_real_name
    , department_id
    , department_name
    , category_id
    , category
    , condition_id
    , deleted
    , COUNT(offer_id) AS number_of_offers_completed

  FROM tracking
  GROUP BY 1,2,3,4,5,6,7,8,9

), combined AS (

  SELECT
      ded.*
    , ts.number_of_offers_started
    , tc.number_of_offers_completed

  FROM date_employee_department AS ded
    LEFT JOIN tracking_started AS ts
      ON ts.started_on = ded.date
      AND ts.employee_id = ded.employee_id
      AND ts.department_id = ded.department_id
      AND ts.category_id = ded.category_id
      AND ts.condition_id = ded.condition_id
      AND ts.deleted = ded.deleted
    LEFT JOIN tracking_completed AS tc
      ON tc.completed_on = ded.date
      AND tc.employee_id = ded.employee_id
      AND tc.department_id = ded.department_id
      AND tc.category_id = ded.category_id
      AND tc.condition_id = ded.condition_id
      AND tc.deleted = ded.deleted
  -- limit the table to dates where both number_of_offers_started
  -- and number_of_offers_completed are not null
  WHERE NOT COALESCE(
    ts.number_of_offers_started,tc.number_of_offers_completed) IS NULL

)

SELECT
  date
  , employee_id 
  , employee_real_name 
  , department_id 
  , department_name 
  , CASE 
        WHEN deleted  = 999999 THEN NULL
        ELSE deleted
    END AS deleted
  , CASE 
        WHEN category_id = 999999 THEN NULL
        ELSE category_id
    END AS category_id
  , CASE 
        WHEN category = 'empty' THEN NULL
        ELSE category
    END
      
  , CASE 
        WHEN condition_id = 999999 THEN NULL
        ELSE condition_id
    END AS condition_id  
 , number_of_offers_started 
 , number_of_offers_completed 

from combined 
