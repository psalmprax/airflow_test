WITH department AS (

  SELECT
      id
    , name AS department_name
    , min
    , max
    , display
    , category
    , goal
    , backlog

  FROM {{ source('scanpoint', 'department') }}

)

SELECT * FROM department
