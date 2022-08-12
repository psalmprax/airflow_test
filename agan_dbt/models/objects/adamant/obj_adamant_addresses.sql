WITH address AS (

  SELECT
      id AS address_id
    , INITCAP(street) AS street
    , additional
    , postal_code
    , INITCAP(city) AS city
    , country_code

  FROM {{ ref ("obj_adamant.address_cleaned") }}

)

SELECT * FROM address