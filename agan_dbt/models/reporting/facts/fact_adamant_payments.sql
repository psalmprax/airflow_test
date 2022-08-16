WITH payments AS (

  SELECT * FROM {{ ref('obj_adamant_payments') }}

)

SELECT * FROM payments
