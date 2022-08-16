/*
 * Customer sensitive information is only available in the object (base) models,
 * with restricted access.
 */
WITH customers AS (

  SELECT
      customer_id
    , orders_count
    , address_id
    , city
    , postal_code
    , country_code

  FROM {{ ref('obj_adamant_customers') }}

)

SELECT * FROM customers
