WITH devices AS (

  SELECT
      d.id AS device_id
    , d.stock_id
    , d.business_channel_id
    , bc.business_channel_name
    , d.reference
    , d.article_number
    , d.order_number
    , d.tax
    , d.name AS device_name
    , d.status
    , d.status_updated_at
    , d.imei
    , d.serial_number
    , COALESCE(d.warranty_permitted=1, FALSE) AS is_warranty_permitted
    , d.created_at
    , d.updated_at
    , d._ewah_executed_at

  FROM {{ ref ("obj_adamant.device_cleaned") }} AS d
    LEFT JOIN {{ ref("obj_adamant_business_channel") }}  AS bc
      ON bc.business_channel_id = d.business_channel_id

)

SELECT * FROM devices