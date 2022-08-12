WITH orders AS (

  SELECT
      o.id AS order_id
    , o.shipping_address_id
    , a.city
    , a.postal_code
    , a.country_code
    , o.business_channel_id
    , bc.business_channel_name
    , o.sales_channel_id
    , sc.sales_channel_name
    , o.customer_id
    , o.invoice_id
    , o.shipping_id
    , o.payment_id
    , o.reference
    , o.status
    , o.created_at
    , o.updated_at
    , o.sold_at
    , o.shipping_price
    , o.invitation_sent_at
    , o.confirmation_sent_at
    , o.comment
--    , o._ewah_executed_at
    , COALESCE(o.exported=1, FALSE) AS is_exported

  FROM {{ ref ("obj_adamant.orders_cleaned") }} AS o
    LEFT JOIN {{ ref ("obj_adamant_addresses") }} AS a
      ON a.address_id = o.shipping_address_id
    LEFT JOIN {{ ref ("obj_adamant_business_channel") }} AS bc
      ON bc.business_channel_id = o.business_channel_id
    LEFT JOIN {{ ref ("obj_adamant_sales_channels") }} AS sc
      ON sc.sales_channel_id = o.sales_channel_id

)

SELECT * FROM orders