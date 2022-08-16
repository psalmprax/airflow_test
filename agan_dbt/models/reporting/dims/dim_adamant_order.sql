
WITH dim_order AS 
(
SELECT 
 
   oao.order_id
   , oaoi.price AS order_value
   , oaoi.tax
   , oao.created_at as order_created
   , oao.country_code 
   , oas.shipping_name as shipping_service
   , oao.shipping_price 
   , oao.customer_id 
   , oao.invoice_id 
   , oap.gateway_name AS payment_method
   , oao.business_channel_name
   , oao.sales_channel_name
   , oap.charge AS payment_costs
 
FROM {{ ref('obj_adamant_orders') }} oao
LEFT JOIN {{ ref('obj_adamant_shippings') }} oas 
  ON oao.shipping_id = oas.shipping_id 
LEFT JOIN {{ ref('obj_adamant_payments') }} oap 
  ON oao.payment_id = oap.payment_id 
LEFT JOIN {{ ref('obj_adamant_order_items') }} oaoi 
  ON oao.order_id = oaoi.order_id 
)
SELECT 
    order_id AS id
   , order_value
   , tax
   , order_created
   , country_code 
   , shipping_service
   , shipping_price 
   , customer_id 
   , invoice_id 
   , payment_method
   , business_channel_name
   , sales_channel_name
   , payment_costs
FROM dim_order

