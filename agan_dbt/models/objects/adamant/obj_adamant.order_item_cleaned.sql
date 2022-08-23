select
	convert(numeric,case id when '' then null else id end) as id,
	convert(numeric,case order_id when '' then null else order_id end) as order_id,
	case tax when '' then null else tax end as tax,
	convert(numeric,case quantity when '' then null else quantity end) as quantity,
	case sku when '' then null else sku end as sku,
	convert(decimal(15,2),case price when '' then null else price end) as price,
	case name when '' then null else name end as name
from
	raw_adamant.order_item