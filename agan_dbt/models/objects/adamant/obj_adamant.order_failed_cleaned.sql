select
	convert(numeric,case id when '' then null else id end) as id,
	case order_number when '' then null else order_number end as order_number,
	case order_json when '' then null else order_json end as order_json,
	case reason when '' then null else reason end as reason
from 
	raw_adamant.order_failed