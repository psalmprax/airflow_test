with orders as
(

SELECT distinct
    order_id 
    , customer_id 
    , created_at
    , extract(year FROM created_at) AS year_order
    , shipping_price 
    , invoice_id
    , status
    , payment_id
    , count(order_id) OVER (PARTITION BY customer_id) AS number_orders
    , min(created_at) OVER (PARTITION BY customer_id) AS first_order

FROM {{ ref('obj_adamant_orders') }}
--WINDOW w_customer AS (PARTITION BY customer_id)
)
, order_items AS
(
SELECT 
  o.*
  , oaoi.order_item_id
  , oaoi.quantity
  , oaoi.price 
  , oaoi.price + o.shipping_price AS total_price
  
FROM orders o 
JOIN {{ ref('obj_adamant_order_items') }} oaoi 
  ON o.order_id = oaoi.order_id 
)
, yearly_sales_order_item AS
(
SELECT distinct
  * 
  , avg(total_price) OVER (PARTITION BY customer_id) AS avg_basket
  , sum(total_price) OVER (PARTITION BY customer_id,extract(year FROM created_at)) AS total_year_sales
FROM order_items
--WINDOW w_customer AS (PARTITION BY customer_id),
--       w_customer_year AS (PARTITION BY customer_id,extract(year FROM created_at))
)
, details_customer_payments AS
(
SELECT 
  ysoi.*
  , oac.email
  , oac.firstname
  , oac.lastname
  , oac.street
  , oac.phone
  , oap.gateway_name
  , oaoid.device_id
  , CASE 
      WHEN oaoid.status IN ('returned','active') THEN 'ja'
      ELSE 'nein'
    END g30_status
FROM yearly_sales_order_item  ysoi
JOIN {{ ref('obj_adamant_order_item_details') }} oaoid 
  ON ysoi.order_item_id = oaoid.order_item_id
JOIN {{ ref('obj_adamant_customers') }} oac 
  ON ysoi.customer_id = oac.customer_id
JOIN {{ ref('obj_adamant_payments') }} oap
  ON ysoi.payment_id = oap.payment_id 


)
, devices AS 
(
SELECT 
  dcp.*
  , oad.device_name
  , oamdc.category_name 
FROM details_customer_payments dcp
JOIN {{ ref('obj_adamant_devices') }} oad 
   ON dcp.device_id = oad.device_id
JOIN {{ ref('obj_adamant_mapping_device_category') }} oamdc 
   ON dcp.device_id = oamdc.device_id

)

SELECT
  order_id AS id
  , customer_id 
  , email
  , firstname 
  , lastname 
  , street 
  , phone 
  , payment_id
  , gateway_name 
  , device_id 
  , device_name 
  , category_name
  , created_at 
  , year_order 
  , invoice_id  
  , order_item_id
  , number_orders 
  , first_order 
  , total_price 
  , avg_basket 
  , total_year_sales 
FROM devices 
