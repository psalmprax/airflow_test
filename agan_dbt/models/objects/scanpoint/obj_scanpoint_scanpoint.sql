WITH scanpoint AS (

  SELECT
      id
    , name

  FROM {{ source('scanpoint', 'scanpoint') }}

)

SELECT * FROM scanpoint
