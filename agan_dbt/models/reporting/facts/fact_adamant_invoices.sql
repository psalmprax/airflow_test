WITH invoices AS (

  SELECT * FROM {{ ref('obj_adamant_invoices') }}

)

SELECT * FROM invoices
