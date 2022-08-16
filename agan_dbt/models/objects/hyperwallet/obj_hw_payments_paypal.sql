SELECT 
    id AS payment_id,
    payment_type_id,
    payment_type,
    paypal_id ,
    NULL AS first_name,
    NULL AS middle_name,
    NULL AS last_name,
    NULL AS swift_iban
    
FROM {{ ref('obj_wkfs_payment') }}
WHERE payment_type_id = 2
