WITH payment AS (

  SELECT
      p.id AS payment_id
    , p.status
    , p.charge
    , p.paid_amount
    , p.created_at
    , p.updated_at
    , p.gateway_id
    , g.gateway_name
    , p.reference

  FROM {{ ref ("obj_adamant.payments_cleaned") }} AS p
    LEFT JOIN {{ ref ("obj_adamant_gateways") }} AS g
      ON g.gateway_id = p.gateway_id

)

SELECT * FROM payment