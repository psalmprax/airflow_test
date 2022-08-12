WITH order_item_details AS (

  SELECT
      d.id AS order_item_detail_id
    , d.order_item_id
    , d.device_id
    , d.status
    , d.warranty_id
    , w.started_at AS warranty_started_at
    , w.finished_at AS warranty_finished_at
    , w.created_at as warranty_created_at
    , w.updated_at as warranty_updated_at
    , w.duration AS warranty_duration

  FROM {{ ref ("obj_adamant.order_item_detail_cleaned") }} AS d
    LEFT JOIN {{ ref ("obj_adamant_warranties") }} AS w
      ON w.warranty_id = d.warranty_id

)

SELECT * FROM order_item_details