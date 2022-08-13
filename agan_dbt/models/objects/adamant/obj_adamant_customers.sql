WITH customers AS (

  SELECT
      c.id AS customer_id
    , INITCAP(c.firstname) AS firstname
    , INITCAP(c.lastname) AS lastname
    , c.email
    , c.phone
    , c.orders_count
    , c.address_id
    , a.street
    , a.city
    , a.postal_code
    , a.country_code
  FROM {{ ref ("obj_adamant.customer_cleaned") }} AS c
    LEFT JOIN {{ ref ("obj_adamant_addresses") }} AS a
      ON a.address_id = c.address_id

)

SELECT * FROM customers
