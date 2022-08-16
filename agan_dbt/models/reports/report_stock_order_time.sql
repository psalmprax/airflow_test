SELECT 
    rs.id
    ,rs.offer_id
    ,fao.reference AS order_number
    ,fao.created_at AS order_created_at
    ,fao.invoice_id 
FROM {{ ref('dim_adamant_devices') }} dad 
LEFT JOIN {{ ref('fact_adamant_orders') }} fao
ON dad.order_number = fao.reference
LEFT JOIN {{ ref('report_stock') }} rs
ON dad.reference = rs.offer_id
