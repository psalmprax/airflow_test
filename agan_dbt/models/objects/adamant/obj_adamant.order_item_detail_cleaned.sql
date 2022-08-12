with results as (
select
	convert(numeric,case id when '' then null else id end) as id,
	convert(numeric,case order_item_id when '' then null else order_item_id end) as order_item_id,
	convert(numeric,case device_id when '' then null else device_id end) as device_id,
	case status when '' then null else status end as status,
	case warranty_id when '' then null else warranty_id end as warranty_id
from
	{{ source("adamant", "order_item_detail") }}
)

select * from results