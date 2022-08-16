WITH employee AS (

  SELECT
      id
    , bo_name
    , real_name
    , COALESCE(scanpoint_admin=1, FALSE) AS is_scanpoint_admin

  FROM {{ source('scanpoint', 'employee') }}

)

SELECT * FROM employee
